# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from struct import pack, unpack
from concurrent.futures import ThreadPoolExecutor
import time
from hashlib import md5
from threading import Timer, Lock


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        # Added fields
        self.sequence_number = 0
        self.expected_sequence_number = 0
        self.expected_ack_number = 0
        self.send_buffer = {}
        self.receive_buffer = {}
        self.in_flight = {}
        self.finished = False
        self.closed = False
        self.close_request = False
        self.receive_buffer_lock = Lock()
        self.in_flight_lock = Lock()

        # Start listener function
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

        # Create Timer
        self.timer = Timer(0.25, self.retransmit)
        self.timer.start()

    def listener(self) -> None:
        while not self.closed: 
            try:
                # Get data and check headers
                data, addr = self.socket.recvfrom() 

                if data:
                    # Checksum for no corruption
                    recv_hash = data[0:16]
                    desired_hash = bytes.fromhex(md5(data[16:]).hexdigest())

                    if desired_hash == recv_hash: 
                        # Get parts of header and data
                        curr_seq_num = unpack("!i", data[16:20])[0]
                        is_data = unpack("!?", data[20:21])[0]
                        body = data[21:]
    
                        if is_data:
                            # Send FIN ACK packet or data ACK packet
                            if body == b"FIN":
                                if curr_seq_num == self.expected_sequence_number:
                                    # Sequence number is an integer
                                    # True = data, False = ACK
                                    fin_packet = pack("!i", curr_seq_num) + pack("!?", False) + b"FIN"
                                    fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
                                    self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                                    self.close_request = True
                            else:
                                with self.receive_buffer_lock:
                                    # Add packet to list of received packets for the recv() function to analyze
                                    if curr_seq_num not in self.receive_buffer and curr_seq_num >= self.expected_sequence_number:
                                        self.receive_buffer[curr_seq_num] = body
                                    elif curr_seq_num < self.expected_sequence_number:
                                        # Sequence number is an integer
                                        # True = data, False = ACK
                                        acknowledgement = pack("!i", curr_seq_num) + pack("!?", False) 
                                        acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                                        self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))
                        else:
                            # Update close state for FIN ACK packet or update expected_ack_number of data
                            if body == b"FIN":
                                self.finished = True
                            else:
                                if curr_seq_num >= self.expected_ack_number:
                                    with self.in_flight_lock:
                                        for i in range(self.expected_ack_number, curr_seq_num + 1):
                                            if i in self.in_flight:
                                                self.in_flight.pop(i)

                                    self.expected_ack_number = curr_seq_num + 1
                                    
                                    # Reset timer since packet has been ACKed
                                    self.timer.cancel()
                                    self.timer = Timer(0.25, self.retransmit)
                                    self.timer.start()

            except Exception as e:
                print("listener died!")
                print(e)
        self.executor.shutdown()

    def retransmit(self):
        if not self.closed:
            # Resend in-flight packets
            for seq_num in range(self.expected_ack_number, self.sequence_number):
                if seq_num in self.in_flight:
                    self.socket.sendto(self.send_buffer[seq_num], (self.dst_ip, self.dst_port))

            # Reset timer
            self.timer.cancel()
            self.timer = Timer(0.25, self.retransmit)
            self.timer.start()
        exit()
            

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # Break data bytes into packets to send
        max_packet_size = 1472 - 4 - 1 - 16    # max_packet_size = 1472 - 4 - 1 - 16
        for i in range(0, len(data_bytes), max_packet_size): 
            # Sequence number is an integer
            # True = data, False = ACK
            packet = pack("!i", self.sequence_number) + pack("!?", True) + data_bytes[i:i + max_packet_size]
            packet = bytes.fromhex(md5(packet).hexdigest()) + packet
            
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            with self.in_flight_lock:
                self.in_flight[self.sequence_number] = packet
            self.send_buffer[self.sequence_number] = packet
            
            self.sequence_number += 1


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!

        while True:
            data = b""
            with self.receive_buffer_lock:
                # Check if next expected sequence number has arrived
                if self.expected_sequence_number in self.receive_buffer:
                    packet_data = self.receive_buffer.pop(self.expected_sequence_number)
                    
                    # Send ACK packet of received data packet
                    acknowledgement = pack("!i", self.expected_sequence_number) + pack("!?", False)
                    acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                    self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))

                    self.expected_sequence_number += 1
                    data += packet_data
            if data:
                return data
            time.sleep(0.01)


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.

        # Wait for in-flight packets to be ACKed
        while len(self.in_flight) > 0:
            time.sleep(0.01)

        # Create FIN packet to send
        fin_packet = pack("!i", self.sequence_number) + pack("!?", True) + b"FIN"
        fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            
        # Resend FIN packet in intervals until it has been ACKed
        timeout = time.time() + 0.25
        while not self.finished and not self.close_request:
            self.close_request = False
            if time.time() > timeout:
                self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                timeout = time.time() + 0.25
            time.sleep(0.01)
        time.sleep(4)

        # Close host
        self.closed = True
        self.timer.cancel()
        self.socket.stoprecv()
