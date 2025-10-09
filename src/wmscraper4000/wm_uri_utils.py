import re

def dissect_wm_memento_uri(uri_m: str) -> dict:
    """
    Dissects a Wayback Machine memento URI into its components.

    Args:
        uri (str): The Web Memory memento URI to dissect.

    Returns:
        dict: A dictionary containing the dissected components of the URI: 
            - uri-r: the original resource URI
            - timestamp: the timestamp of the memento
            - rewrite_modifier: the rewrite modifier (if any)
    """
    
    # Example URI: http://web.archive.org/web/20220101000000id_/http://example.com, 
    # where:
    # - 20220101000000 is the timestamp, and timestamp is not always 14 digits long
    # - id_ is the rewrite modifier (optional)
    # - http://example.com is the original resource URI. The original resource URI can be any valid URI. The original URI should not start with a slash.
    # - The input URI can be http or https, but don't output the scheme (http or https) of the Wayback Machine URI
    # - The uri-r can contain http, https, or no scheme (e.g., example.com)
    # - The original resource URI can contain query parameters and fragments
    # - The rewrite modifier can be any two characters followed by an underscore
    # - 

    pattern = re.compile(
        r"^https?://web\.archive\.org/web/(?P<timestamp>\d{1,14})(?P<rewrite_modifier>[a-zA-Z0-9]{2}_)?/(?P<uri_r>.+)$"
    )
    match = pattern.match(uri_m)
    if not match:
        raise ValueError("Invalid Wayback Machine memento URI format")
    
    return match.groupdict()


def create_wm_memento_uri(uri_r: str, timestamp: str, rewrite_modifier: str = "", use_https: bool = True) -> str:
    """
    Creates a Wayback Machine memento URI from its components.

    Args:
        uri_r (str): The original resource URI.
        timestamp (str): The timestamp of the memento.
        rewrite_modifier (str, optional): The rewrite modifier. Defaults to an empty string.

    Returns:
        str: The constructed Wayback Machine memento URI.
    """
    if rewrite_modifier and not re.match(r"^[a-zA-Z0-9]{2}_$", rewrite_modifier):
        raise ValueError("Invalid rewrite modifier format")
    
    return f"http://web.archive.org/web/{timestamp}{rewrite_modifier}/{uri_r}"

if __name__ == "__main__":
    # Example usage
    uri = "http://web.archive.org/web/20000101000010id_/https://example.com?query=1#fragment"
    components = dissect_wm_memento_uri(uri)
    print(components)

    