def get_value_by_path(data, path):
    """
    Extract a value from nested data structures using a path of keys/indices.
    
    Recursively navigates through dictionaries and lists using a path sequence.
    Returns None if any key is missing or index is out of bounds.
    
    Args:
        data (dict or list): Nested data structure to traverse
        path (list): Sequence of keys (for dicts) or indices (for lists)
        
    Returns:
        any: The value at the path location, or None if not found
    """
    current = data

    for key in path:
        if isinstance(current, dict):
            if key not in current:
                return None
            current = current[key]

        elif isinstance(current, list):
            if not isinstance(key, int):
                return None
            if key < 0 or key >= len(current):
                return None
            current = current[key]

        else:
            return None

    return current