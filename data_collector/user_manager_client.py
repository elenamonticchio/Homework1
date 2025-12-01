import os
import grpc

import user_manager_pb2
import user_manager_pb2_grpc

USER_MGR_GRPC_HOST = os.getenv("USER_MGR_GRPC_HOST", "user-manager")
USER_MGR_GRPC_PORT = os.getenv("USER_MGR_GRPC_PORT", "50051")

def user_exists(email: str) -> bool:
    target = f"{USER_MGR_GRPC_HOST}:{USER_MGR_GRPC_PORT}"

    try:
        with grpc.insecure_channel(target) as channel:
            stub = user_manager_pb2_grpc.UserServiceStub(channel)
            request = user_manager_pb2.CheckUserRequest(email=email)
            response = stub.CheckUser(request, timeout=3)
            return response.exists
    except grpc.RpcError as e:
        print(f"[gRPC client] Errore nel contattare User Manager: {e}")
        return False
