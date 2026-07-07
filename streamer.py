# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.HEADER_SIZE=4
        self.MAX_PAYLOAD = 1472 - self.HEADER_SIZE
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.packet={}
        self.next_seq=0
        self.send_seq=0

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        for i in range(0, len(data_bytes), self.MAX_PAYLOAD):
            chunk = data_bytes[i : i + self.MAX_PAYLOAD]
            
            header = struct.pack("!I", self.send_seq)
            packet = header + chunk
            
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            
            self.send_seq += 1
        # for now I'm just sending the raw application-level data in one UDP payload
        

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while True:
            
            if self.next_seq in self.packet:
                chunk = self.packet.pop(self.next_seq)
                self.next_seq += 1
                return chunk
            
            # 2. If not in buffer, wait for the network
            data, _ = self.socket.recvfrom()
            
            if len(data) < self.HEADER_SIZE:
                continue
                
            header = data[:self.HEADER_SIZE]
            chunk = data[self.HEADER_SIZE:]
            
            # Unpack the sequence number
            seq = struct.unpack("!I", header)[0]
            
            # 3. Handle the sequence logic
            if seq == self.next_seq:
                # Perfect! Deliver it immediately.
                self.next_seq += 1
                return chunk
            elif seq > self.next_seq:
                # It's a future packet. Stash it safely in the buffer.
                self.packet[seq] = chunk

        # For now, I'll just pass the full UDP payload to the app
       

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
