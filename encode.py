import base64

filename = 'image.jpg'

def encode_file_to_base64(filename):
    try:
        with open(filename, 'rb') as binary_file:
            binary_data = binary_file.read()
            encoded_data = base64.b64encode(binary_data)
            base64_message = encoded_data.decode('utf-8')

        return base64_message

    except FileNotFoundError:
        return f"Error: File '{filename}' not found."


result = encode_file_to_base64(filename)

