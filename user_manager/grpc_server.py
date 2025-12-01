import os
import threading
from concurrent import futures

import grpc
from mysql.connector import Error

from db import get_connection
import user_manager_pb2
import user_manager_pb2_grpc

class UserService(user_manager_pb2_grpc.UserServiceServicer):
    def CheckUser(self, request, context):
        email = request.email
        exists = False
        conn = None

        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM users WHERE email = %s", (email,))
            exists = cursor.fetchone() is not None

        except Error as e:
            print(f"[gRPC CheckUser] Errore DB: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Errore database nel controllo utente")
            return user_manager_pb2.CheckUserResponse(exists=False)

        finally:
            if conn:
                conn.close()

        return user_manager_pb2.CheckUserResponse(exists=exists)


def start_grpc_server():
    port = os.getenv("GRPC_PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_manager_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port(f"[::]:{port}")
    print(f"[gRPC] UserService in ascolto sulla porta {port}")
    server.start()
    server.wait_for_termination()


def start_grpc_server_in_background():
    t = threading.Thread(target=start_grpc_server, daemon=True)
    t.start()
