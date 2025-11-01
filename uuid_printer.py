import gdb

# Pretty-printer class for uuids::uuid
class UuidPrinter:
    "Pretty-printer for uuids::uuid"

    def __init__(self, val):
        self.val = val

    def to_string(self):
        # A pointer `uuids::uuid *` needs to be dereferenced to get the `uuids::uuid` object
        if self.val.type.code == gdb.TYPE_CODE_PTR:
            val = self.val.dereference()
        else:
            val = self.val

        # The GDB pretty printer uses a Python string to represent the formatted output.
        # This part of the code formats the bytes into the canonical UUID string representation.
        # It's better to do the formatting directly in the Python script.
        try:
            addr = val.address

            data_bytes = gdb.selected_inferior().read_memory(addr, 16).tobytes()

            hex_string = "".join([f"{byte:02x}" for byte in data_bytes])

            return f"{hex_string[0:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:32]}"
        except Exception as e:
            return f"Error unpacking UUID bytes: {e}"

    def display_hint(self):
        return 'string'

# Lookup function: returns UuidPrinter for uuids::uuid objects
def uuid_lookup(val):
    # Normalize the type string to handle const, volatile, etc.
    type_name = str(val.type.unqualified())
    if type_name == "uuids::uuid" or type_name == "uuids::uuid *" or type_name == "hedge::key_t":
        return UuidPrinter(val)
    return None

# Register the pretty-printer
gdb.pretty_printers.append(uuid_lookup)