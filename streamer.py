# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from struct import pack, unpack
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

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        max_packet_size = 1472 - 4
        chunks = []
        for i in range(0, len(data_bytes), max_packet_size): 
            packet = pack("!i", self.sequence_number) + data_bytes[i:i + max_packet_size]
            self.sequence_number += 1

            chunks.append(packet)
        # l = len(data_bytes)
        # i = 0
        # chunks = []
        # while l > max_packet_size:
        #     chunks.append(data_bytes[i:i+max_packet_size])
        #     l -= max_packet_size
        #     i += max_packet_size
        # chunks.append(data_bytes[i:])
        
        for chunk in chunks:
            self.socket.sendto(chunk, (self.dst_ip, self.dst_port))
            # for now I'm just sending the raw application-level data in one UDP payload
            # self.socket.sendto(data_bytes, (self.dst_ip, self.dst_port))
            

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        

        while True:
            data, addr = self.socket.recvfrom()
            curr_seq_num = unpack("!i", data[0:4])[0]
            # print("Expected: " + str(self.expected_sequence_number))
            # print("Curr: " + str(curr_seq_num))
            # print(self.receive_buffer)

            self.receive_buffer[curr_seq_num] = data[4:]

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
        pass
