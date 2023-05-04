from cryptography.fernet import Fernet

def generate_random_fernet_keys():
    print(*[Fernet.generate_key().decode('utf-8') for _ in range(10)])], sep='\n\n')