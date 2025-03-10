import json
import os
import subprocess
from typing import Any, Dict, List, Optional, Union
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf import json_format
from google.protobuf.json_format import ParseError
from google.protobuf.struct_pb2 import Struct

class GenericJsonProtobufSerializer:
    """
    A generalized serializer that can convert any JSON data to Protocol Buffers format
    using the google.protobuf.Struct message type, which can represent arbitrary JSON.
    
    This serializer doesn't require predefined .proto files as it uses the dynamic
    Struct message type from Protocol Buffers.
    """
    
    def __init__(self, input_file_path: str = "complex.json", 
                 protobuf_output_path: str = "complex-output.pb",
                 json_output_path: str = "output_from_protobuf.json"):
        """
        Initialize the serializer with the path to the data files.
        
        Args:
            input_file_path: Path to the input JSON data file
            protobuf_output_path: Path to the output protobuf data file
            json_output_path: Path for JSON output when converting from protobuf back to JSON
        """
        self.input_file_path = input_file_path
        self.protobuf_output_path = protobuf_output_path
        self.json_output_path = json_output_path
        self.data = None
        self.proto_data = None
        
        # Ensure protobuf is installed
        try:
            import google.protobuf
        except ImportError:
            print("Installing protobuf package...")
            subprocess.check_call(["pip", "install", "protobuf"])
    
    def load_data(self) -> Any:
        """
        Load and parse the JSON data from the input file.
        
        Returns:
            The parsed JSON data
        
        Raises:
            FileNotFoundError: If the data file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        try:
            with open(self.input_file_path, 'r', encoding='utf-8') as file:
                self.data = json.load(file)
            return self.data
        except FileNotFoundError:
            raise FileNotFoundError(f"Data file not found: {self.input_file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in {self.input_file_path}: {str(e)}", e.doc, e.pos)
    
    def json_to_protobuf(self, json_data: Optional[Any] = None) -> Struct:
        """
        Convert JSON data to Protocol Buffers format using google.protobuf.Struct.
        
        Args:
            json_data: Optional JSON data to convert. If None, uses the loaded data.
            
        Returns:
            The Protocol Buffers Struct message
            
        Raises:
            ValueError: If no data is provided and none has been loaded
            ParseError: If the JSON cannot be converted to protobuf
        """
        if json_data is None:
            if self.data is None:
                raise ValueError("No data to convert. Call load_data() first or provide json_data.")
            json_data = self.data
        
        # Create a new Struct message
        proto_struct = Struct()
        
        try:
            # Convert JSON to Struct
            json_format.ParseDict(json_data, proto_struct)
            self.proto_data = proto_struct
            return proto_struct
        except ParseError as e:
            raise ParseError(f"Failed to convert JSON to protobuf: {str(e)}")
    
    def save_protobuf(self) -> None:
        """
        Save data to the output Protocol Buffers file.
        
        Raises:
            ValueError: If no data has been converted to protobuf
        """
        if not self.proto_data:
            # Try to convert if we have JSON data but haven't converted yet
            if self.data:
                self.json_to_protobuf()
            else:
                raise ValueError("No data to save. Call load_data() and json_to_protobuf() first.")
        
        # Serialize to binary format
        with open(self.protobuf_output_path, 'wb') as file:
            file.write(self.proto_data.SerializeToString())
    
    def load_protobuf(self) -> Struct:
        """
        Load data from a Protocol Buffers file.
        
        Returns:
            The Protocol Buffers Struct message
            
        Raises:
            FileNotFoundError: If the protobuf file doesn't exist
        """
        if not os.path.exists(self.protobuf_output_path):
            raise FileNotFoundError(f"Protobuf file not found: {self.protobuf_output_path}")
        
        # Create a new Struct message
        proto_struct = Struct()
        
        # Read the existing file
        with open(self.protobuf_output_path, 'rb') as file:
            proto_struct.ParseFromString(file.read())
        
        self.proto_data = proto_struct
        return proto_struct
    
    def protobuf_to_json(self) -> Any:
        """
        Convert Protocol Buffers data to JSON format.
        
        Returns:
            The JSON data
            
        Raises:
            ValueError: If no protobuf data has been loaded
        """
        if not self.proto_data:
            raise ValueError("No protobuf data to convert. Call load_protobuf() first.")
        
        # Convert Struct to JSON
        json_data = json_format.MessageToDict(self.proto_data)
        self.data = json_data
        return json_data
    
    def save_json(self, output_file_path: Optional[str] = None) -> None:
        """
        Save the JSON data to a file.
        
        Args:
            output_file_path: Path to the output JSON file. If None, uses the default path.
            
        Raises:
            ValueError: If no JSON data is available
        """
        if output_file_path is None:
            output_file_path = self.json_output_path
            
        if not self.data:
            # Try to convert from protobuf if available
            if self.proto_data:
                self.protobuf_to_json()
            else:
                raise ValueError("No data to save. Load or convert data first.")
        
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(self.data, file, indent=2, ensure_ascii=False)
    
    def convert_string(self, json_string: str) -> bytes:
        """
        Convert a JSON string directly to Protocol Buffers binary format.
        
        Args:
            json_string: The JSON string to convert
            
        Returns:
            The Protocol Buffers binary data
            
        Raises:
            json.JSONDecodeError: If the JSON string is invalid
            ParseError: If the JSON cannot be converted to protobuf
        """
        # Parse the JSON string
        json_data = json.loads(json_string)
        
        # Convert to protobuf
        proto_struct = self.json_to_protobuf(json_data)
        
        # Return the serialized binary data
        return proto_struct.SerializeToString()
    
    def convert_file(self, input_path: str, output_path: str) -> None:
        """
        Convert a JSON file to a Protocol Buffers file.
        
        Args:
            input_path: Path to the input JSON file
            output_path: Path to the output Protocol Buffers file
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            json.JSONDecodeError: If the input file contains invalid JSON
            ParseError: If the JSON cannot be converted to protobuf
        """
        # Save the current paths
        original_input = self.input_file_path
        original_output = self.protobuf_output_path
        
        try:
            # Set the new paths
            self.input_file_path = input_path
            self.protobuf_output_path = output_path
            
            # Load and convert
            self.load_data()
            self.json_to_protobuf()
            self.save_protobuf()
        finally:
            # Restore the original paths
            self.input_file_path = original_input
            self.protobuf_output_path = original_output


# Example usage
if __name__ == "__main__":
    serializer = GenericJsonProtobufSerializer("complex.json", "complex-output.pb")
    try:
        # Load data from JSON
        data = serializer.load_data()
        print(f"Loaded JSON data from {serializer.input_file_path}")
        
        # Convert to Protocol Buffers
        proto_data = serializer.json_to_protobuf()
        print(f"Converted data to Protocol Buffers format")
        
        # Save to Protocol Buffers file
        serializer.save_protobuf()
        print(f"Data successfully serialized to Protocol Buffers: {serializer.protobuf_output_path}")
        
        # Example: Load from Protocol Buffers file and convert back to JSON
        serializer.load_protobuf()
        json_data = serializer.protobuf_to_json()
        serializer.save_json()
        print(f"Converted Protocol Buffers data back to JSON and saved to {serializer.json_output_path}")
        
        # Example: Convert a specific file
        # serializer.convert_file("another_complex.json", "another_complex-output.pb")
        # print("Converted another_complex.json to another_complex-output.pb")
        
    except Exception as e:
        print(f"Error: {e}")
