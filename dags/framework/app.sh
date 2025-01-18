#!/bin/bash

# Default paths
BASE_PATH="./dags/script" 
PYTHON_TEMPLATE="./template/template.py"  # Template Python file for replacement

# Helper function for displaying usage instructions
usage() {
  echo "Usage: $0 --env <script/file_name.py> --build"
  echo "  --env: The relative path of the generated Python file (e.g., script/PRCS_TEST_FRAMEWORK1.py)"
  echo "  --build: Command to indicate the file generation process"
  exit 1
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --env)
      ENV_PATH="$2"
      shift
      ;;
    --build)
      BUILD=true
      ;;
    *)
      echo "Unknown parameter: $1"
      usage
      ;;
  esac
  shift
done

# Validate arguments
if [[ -z "$ENV_PATH" || -z "$BUILD" ]]; then
  echo "Error: Missing required arguments."
  usage
fi

# Extract file name and directory
FILE_NAME=$(basename "$ENV_PATH")
DIR_NAME=$(dirname "$ENV_PATH")
PRCS_NAME=$(basename "$FILE_NAME" .py)  # Extract PRCS_NAME from the file name
FULL_PATH="${BASE_PATH}/${FILE_NAME}"

# Create the script directory if it doesn't exist
mkdir -p "$BASE_PATH"

# Generate the Python file
if [[ "$BUILD" == true ]]; then
  echo "Building Python file at: $FULL_PATH"

  # Replace the placeholder value in the template
  sed "s/{{PRCS_NAME}}/${PRCS_NAME}/g" "$PYTHON_TEMPLATE" > "$FULL_PATH"

  if [[ $? -eq 0 ]]; then
    echo "Python file successfully created: $FULL_PATH"
  else
    echo "Error: Failed to generate Python file."
    exit 1
  fi
fi
