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
        # self.ack = False
        self.ack_buffer = set()
        self.finished = False
        self.closed = False
        self.receive_buffer_lock = Lock()
        self.in_flight_lock = Lock()

        # Start listener function
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

        self.timer = Timer(0.25, self.retransmit)
        self.timer.start()

    def listener(self) -> None:
        while not self.closed: 
            try:
                # Get data and check headers
                data, addr = self.socket.recvfrom() # BLOCKS HERE

                if data:
                    recv_hash = data[0:16] # unpack("!32s", --- )[0]
                    
                    desired_hash = bytes.fromhex(md5(data[16:]).hexdigest())
                    # print("desired: " + str(desired_hash))
                    # print("lalalal: " + str(hash))
                    if desired_hash == recv_hash: 
                        curr_seq_num = unpack("!i", data[16:20])[0]
                        is_data = unpack("!?", data[20:21])[0]
                        body = data[21:]
    
                        if is_data:
                            if body == b"FIN":
                                if curr_seq_num == self.expected_sequence_number:
                                    fin_packet = pack("!i", curr_seq_num) + pack("!?", False) + b"FIN"
                                    fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
                                    self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))

                                    self.finished = True
                            else:
                                with self.receive_buffer_lock:
                                    if curr_seq_num not in self.receive_buffer and curr_seq_num >= self.expected_sequence_number:
                                        self.receive_buffer[curr_seq_num] = body
                            # Sequence number is an integer
                            # True = data, False = ACK
                            # else:
                            #     if curr_seq_num < self.expected_sequence_number - 1:
                            #         acknowledgement = pack("!i", curr_seq_num) + pack("!?", False) #+ body
                            #         acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                            #         self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))
                                    # print(curr_seq_num)
                                    # print(self.expected_ack_number)
                                    # print(self.expected_sequence_number)
                                # else:
                                #     acknowledgement = pack("!i", self.expected_ack_number) + pack("!?", False) #+ body
                                #     acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                                #     self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))
                                
                                # else:
                                #     if body == b"FIN" and curr_seq_num == self.expected_sequence_number:
                                #         fin_packet = pack("!i", curr_seq_num) + pack("!?", False) + b"FIN"
                                #         fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
                                #         self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                        else:
                                
                            # self.ack = True
                            # self.ack_buffer.add(curr_seq_num)
                            if curr_seq_num >= self.expected_ack_number:
                                # for i in range(self.expected_ack_number, curr_seq_num):
                                #     self.send_buffer.pop(self.send_buffer[i])
                                #     print(self.send_buffer)
                                with self.in_flight_lock:
                                    print('hiiii')
                                    # if curr_seq_num in self.in_flight:
                                    print(len(self.in_flight))
                                    for i in range(self.expected_ack_number, curr_seq_num + 1):
                                        if i in self.in_flight:
                                            self.in_flight.pop(i)
                                    print(len(self.in_flight))

                                self.expected_ack_number = curr_seq_num + 1
                                # print(self.expected_ack_number)
                                
                                self.timer.cancel()
                                self.timer = Timer(0.25, self.retransmit)
                                self.timer.start()

                            if body == b"FIN":
                                # fin_packet = pack("!i", curr_seq_num) + pack("!?", False) + b"FIN"
                                # fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
                                # self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))

                                self.finished = True

            except Exception as e:
                print("listener died!")
                print(e)
        self.executor.shutdown()

    def retransmit(self):
        # if self.closed:
        #     # self.timer.cancel()
        #     # self.timer = Timer(0.25, self.retransmit)
        #     # self.timer.start()
        #     exit()
        # else:
        if not self.closed:
            # print("hiiiiiiiiiiiiiiiiiiiii")
            # print(self.expected_ack_number)
            # print(self.sequence_number)
            for seq_num in range(self.expected_ack_number, self.sequence_number):
                if seq_num in self.in_flight:
                    self.socket.sendto(self.send_buffer[seq_num], (self.dst_ip, self.dst_port))
            self.timer.cancel()
            self.timer = Timer(0.25, self.retransmit)
            self.timer.start()
        exit()
            

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # while self.sequence_number - self.expected_ack_number >= 100000:
        #     time.sleep(0.01)

        max_packet_size = 1472 - 4 - 1 - 16    # max_packet_size = 1472 - 4 - 1 - 32
        for i in range(0, len(data_bytes), max_packet_size): 
            # Sequence number is an integer
            # True = data, False = ACK
            packet = pack("!i", self.sequence_number) + pack("!?", True) + data_bytes[i:i + max_packet_size]
            packet = bytes.fromhex(md5(packet).hexdigest()) + packet
            
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.ack = False

            with self.in_flight_lock:
                self.in_flight[self.sequence_number] = packet
            self.send_buffer[self.sequence_number] = packet
            
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
                


            # chunks.append(packet)

        # l = len(data_bytes)
        # i = 0
        # chunks = []
        # while l > max_packet_size:
        #     chunks.append(data_bytes[i:i+max_packet_size])
        #     l -= max_packet_size
        #     i += max_packet_size
        # chunks.append(data_bytes[i:])
        
        # for chunk in chunks:
        #     self.socket.sendto(chunk, (self.dst_ip, self.dst_port))
            # for now I'm just sending the raw application-level data in one UDP payload
            # self.socket.sendto(data_bytes, (self.dst_ip, self.dst_port))

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
            with self.receive_buffer_lock:
                if self.expected_sequence_number in self.receive_buffer:
                    packet_data = self.receive_buffer.pop(self.expected_sequence_number)

                    acknowledgement = pack("!i", self.expected_sequence_number) + pack("!?", False) + packet_data
                    acknowledgement = bytes.fromhex(md5(acknowledgement).hexdigest()) + acknowledgement
                    self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))

                    self.expected_sequence_number += 1
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

        # if not self.ack:
        #     fin_packet = pack("!i", self.sequence_number) + pack("!?", False) + b"FIN"
        #     self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        while len(self.receive_buffer) > 0:
            time.sleep(0.01)
        # while self.sequence_number > self.expected_ack_number:
        #     time.sleep(0.01)
        #     print("HIIii")
        #     print(self.expected_sequence_number)
        #     print(self.expected_ack_number)

        # while len(self.in_flight) > 0:
        #     # print(self.in_flight)
        #     time.sleep(0.01)

        fin_packet = pack("!i", self.sequence_number) + pack("!?", False) + b"FIN"
        fin_packet = bytes.fromhex(md5(fin_packet).hexdigest()) + fin_packet
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            
        timeout = time.time() + 0.25
        while not self.finished:
            if time.time() > timeout:
                self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                timeout = time.time() + 0.25
            print(self.finished)
            time.sleep(0.01)
        
        time.sleep(4)

        self.closed = True
        self.timer.cancel()
        self.executor.shutdown()
        self.socket.stoprecv()
