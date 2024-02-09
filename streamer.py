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
        self.receive_buffer = {}
        self.ack = False
        self.ack_buffer = set()
        self.finished = False
        self.closed = False
        self.window_size = 30
        self.lock = Lock()

        # Start listener function
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)


    def listener(self) -> None:
        while not self.closed: 
            try:
                # Get data and check headers
                data, addr = self.socket.recvfrom()
                time.sleep(0.01)

                if data:
                    hash = data[0:16] # unpack("!32s", --- )[0]
                    
                    desired_hash = bytes.fromhex(md5(data[16:]).hexdigest())
                    # print("desired: " + str(desired_hash))
                    # print("lalalal: " + str(hash))
                    if desired_hash == hash: 
                        curr_seq_num = unpack("!i", data[16:20])[0]
                        is_data = unpack("!?", data[20:21])[0]
                        body = data[21:]

                        
                        if is_data:
                            self.receive_buffer[curr_seq_num] = body
                            # Sequence number is an integer
                            # True = data, False = ACK
                            acknowledgement = pack("!i", curr_seq_num) + pack("!?", False) + body
                            acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                            self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))
                        else:
                            # self.ack = True

                            # if curr_seq_num < self.expected_sequence_number + self.window_size:
                            self.lock.acquire()
                            self.ack_buffer.add(curr_seq_num)
                            self.lock.release()

                            if body == b"FIN":
                                self.finished = True

            except Exception as e:
                print("listener died!")
                print(e)

    def sendWithTimer(self, sequence_number, packet):
        print(sequence_number)
        print(self.ack_buffer)
        if sequence_number in self.ack_buffer:
            self.lock.acquire()
            self.ack_buffer.remove(sequence_number)
            if self.expected_ack_number <= sequence_number:
                self.expected_ack_number = sequence_number + 1
            self.lock.release()
            exit()
        else:
            if sequence_number >= self.expected_ack_number:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                Timer(1, self.sendWithTimer, (sequence_number, packet)).start()
                time.sleep(0.01)
        

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # while self.expected_ack_number - self.sequence_number > self.window_size:
        #     time.sleep(0.01)
        #     print(self.expected_ack_number)
        #     print(self.sequence_number)

        max_packet_size = 1472 - 4 - 1 - 16    # max_packet_size = 1472 - 4 - 1 - 32
        for i in range(0, len(data_bytes), max_packet_size): 
            # Sequence number is an integer
            # True = data, False = ACK
            packet = pack("!i", self.sequence_number) + pack("!?", True) + data_bytes[i:i + max_packet_size]
            packet = bytes.fromhex(md5(packet).hexdigest()) + packet

            while self.expected_ack_number - self.sequence_number > self.window_size:
                time.sleep(0.01)
                # print(self.sequence_number)
            # time.sleep(0.1)

            self.sendWithTimer(self.sequence_number, packet)
            # self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.ack = False
            self.sequence_number += 1

            # time.sleep(0.01)

            # timeout = time.time() + 0.25
            # while not self.ack:
            #     if time.time() > timeout:
            #         self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            #         timeout = time.time() + 0.25
            #     time.sleep(0.01)

            #     if self.closed:
            #         break
                

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        while True:
            # data, addr = self.socket.recvfrom()
            # curr_seq_num = unpack("!i", data[0:4])[0]
            # self.receive_buffer[curr_seq_num] = data[4:]

            # print("Expected: " + str(self.expected_sequence_number))
            # print("Curr: " + str(curr_seq_num))
            # print(self.receive_buffer)

            data = b""
            while self.expected_sequence_number in self.receive_buffer:
                print(self.receive_buffer)
                packet_data = self.receive_buffer.pop(self.expected_sequence_number)
                self.expected_sequence_number += 1
                # return packet_data
                data += packet_data

            if data:
                return data

            time.sleep(0.1)

            # else:
            #     self.receive_buffer[curr_seq_num] = data[4:]
                # time.sleep(0.01)
                


        # this sample code just calls the recvfrom method on the LossySocket
        # data, addr = self.socket.recvfrom()

        # For now, I'll just pass the full UDP payload to the app
        # return data


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.

        while len(self.ack_buffer) != 0:
            time.sleep(0.1)

        # if not self.ack:
        #     fin_packet = pack("!i", self.sequence_number) + pack("!?", False) + b"FIN"
        #     self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))

        fin_packet = pack("!i", self.sequence_number) + pack("!?", False) + b"FIN"
        fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            
        timeout = time.time() + 0.25
        while not self.finished:
            if time.time() > timeout:
                self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                timeout = time.time() + 0.25
            time.sleep(0.01)
        
        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()