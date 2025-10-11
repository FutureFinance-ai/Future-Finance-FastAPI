import secrets
import string

# Define the character set to use
# This example uses a strong character set including letters, digits, and punctuation
alphabet = string.ascii_letters + string.digits + string.punctuation

# Generate a 256-bit (32-byte) random token and convert it to a hex string.
# Each hex character represents 4 bits, so 32 bytes (256 bits) will result in 64 hex characters.
random_hex_string = secrets.token_hex(32)

# If a string of a specific length using a custom character set is desired,
# calculate the required length based on the character set size.
# For example, to get a string with at least 256 bits of entropy using the 'alphabet' defined above:
# log2(len(alphabet)) gives the bits per character.
# 256 / log2(len(alphabet)) gives the required number of characters.
# For a practical example, let's generate a string of 64 characters from the alphabet:
random_char_string = ''.join(secrets.choice(alphabet) for i in range(64))

print(f"256-bit equivalent random hex string (64 characters): {random_hex_string}")
print(f"Random string using a custom alphabet (64 characters): {random_char_string}")