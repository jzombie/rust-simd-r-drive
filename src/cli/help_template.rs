use indoc::indoc;

// Help text template with placeholder
pub const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      # Writing a value explicitly
      %BINARY_NAME% data.bin write mykey "Hello, world!"

      # Writing a value from stdin
      echo "Hello, world!" | %BINARY_NAME% data.bin write mykey
      cat file.txt | %BINARY_NAME% data.bin write mykey

      # Reading a value
      %BINARY_NAME% data.bin read mykey

      # Reading a value with a 32KB buffer size
      %BINARY_NAME% data.bin read mykey --buffer-size 32K

      # Copying a key to another storage file
      %BINARY_NAME% data.bin copy mykey target_data.bin

      # Moving a key to another storage file (deletes from source)
      %BINARY_NAME% data.bin move mykey target_data.bin

      # Renaming a key from 'old_key' to 'new_key'
      %BINARY_NAME% data.bin rename old_key new_key

      # Deleting a key
      %BINARY_NAME% data.bin delete mykey

      # Compacting the storage file
      %BINARY_NAME% data.bin compact

      # Displaying storage file info
      %BINARY_NAME% data.bin info

      # Retrieving metadata for a specific key
      %BINARY_NAME% data.bin metadata mykey
"#};
