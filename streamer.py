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
        self.HEADER_SIZE=5
        self.MAX_PAYLOAD = 1472 - self.HEADER_SIZE
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.packet={}

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        chunks=[data_bytes[i:i+self.MAX_PAYLOAD] for i in range(0,len(data_bytes),self.MAX_PAYLOAD)]

        for seq,chunk in enumerate(chunks):
            is_last=1 if seq==len(chunks)-1 else 0
            # print(seq,chunks)
            header=struct.pack("!IB",seq,is_last)
            packet=header+chunk
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        # for now I'm just sending the raw application-level data in one UDP payload
        

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        last_seq=-5
        while True:
            data, _ = self.socket.recvfrom()
            
            header=data[:self.HEADER_SIZE]
            seq,last=struct.unpack("!IB",header)
            
            chunk=data[self.HEADER_SIZE:]
            self.packet[seq]=chunk

            if last==1:
                last_seq=seq

            while len(self.packet)-1==last_seq:
                msg=bytes()
                for seq in sorted(self.packet):
                    msg += self.packet[seq]
                return msg

        # For now, I'll just pass the full UDP payload to the app
       

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
