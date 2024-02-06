# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from struct import pack, unpack
from concurrent.futures import ThreadPoolExecutor
import time


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
        self.receive_buffer = {}
        self.ack = False
        # self.ack_buffer = set()
        self.closed = False

        # Start listener function
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def listener(self) -> None:
        while not self.closed: 
            try:
                # Get data and check headers
                data, addr = self.socket.recvfrom()
                curr_seq_num = unpack("!i", data[0:4])[0]
                is_data = unpack("!?", data[4:5])[0]
                body = data[5:]

                if is_data:
                    self.receive_buffer[curr_seq_num] = body
                    # Sequence number is an integer
                    # True = data, False = ACK
                    acknowledgement = pack("!i", self.sequence_number) + pack("!?", False) + body
                    self.socket.sendto(acknowledgement, (self.dst_ip, self.dst_port))
                else:
                    self.ack = True
                    # self.ack_buffer.add(curr_seq_num)

            except Exception as e:
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        max_packet_size = 1472 - 4 - 1    # max_packet_size = 1472 - 4 - 1
        for i in range(0, len(data_bytes), max_packet_size): 
            # Sequence number is an integer
            # True = data, False = ACK
            packet = pack("!i", self.sequence_number) + pack("!?", True) + data_bytes[i:i + max_packet_size]
            self.sequence_number += 1

            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.ack = False

            counter = 0
            while not self.ack:
                time.sleep(0.001)
                if counter > 1000:
                    break
                counter += 1
                


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
            while self.expected_sequence_number in self.receive_buffer:
                packet_data = self.receive_buffer.pop(self.expected_sequence_number)
                self.expected_sequence_number += 1
                # return packet_data
                data += packet_data

            if data:
                return data

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
        self.closed = True
        self.socket.stoprecv()

