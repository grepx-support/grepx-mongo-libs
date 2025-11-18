"""MongoDB URI Parsing and Building"""

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Any
from dataclasses import dataclass, field


@dataclass
class MongoDBURI:
    """Parsed MongoDB URI"""

    scheme: str = "mongodb"
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    hosts: list[tuple[str, int | None]] | None = None  # For replica sets
    extra_params: dict[str, Any] = field(default_factory=dict)

    def to_uri(self) -> str:
        """Convert back to URI string"""
        return build_uri(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            hosts=self.hosts,
            **self.extra_params
        )

    def to_dsn(self) -> str:
        """Convert to connection DSN string"""
        parts = []
        if self.host:
            parts.append(f"host={self.host}")
        if self.port:
            parts.append(f"port={self.port}")
        if self.database:
            parts.append(f"database={self.database}")
        if self.user:
            parts.append(f"username={self.user}")
        if self.password:
            parts.append(f"password={self.password}")
        
        for key, value in self.extra_params.items():
            parts.append(f"{key}={value}")
        
        return " ".join(parts)


def parse_uri(uri: str) -> MongoDBURI:
    """
    Parse MongoDB URI
    
    Format: mongodb://user:password@host:port/database?param=value
    Also supports: mongodb+srv:// for MongoDB Atlas
    """
    parsed = urlparse(uri)

    # Support both mongodb:// and mongodb+srv://
    if parsed.scheme not in ("mongodb", "mongodb+srv"):
        raise ValueError(f"Invalid scheme: {parsed.scheme}. Expected 'mongodb' or 'mongodb+srv'")

    # Parse query parameters
    params = parse_qs(parsed.query, keep_blank_values=True)
    extra_params = {k: v[0] if len(v) == 1 else v for k, v in params.items()}

    # Parse hosts (can be comma-separated for replica sets)
    hosts = None
    if parsed.hostname:
        host_parts = parsed.hostname.split(',')
        hosts = []
        for host_part in host_parts:
            if ':' in host_part:
                host, port_str = host_part.rsplit(':', 1)
                try:
                    port = int(port_str)
                except ValueError:
                    port = None
                hosts.append((host, port))
            else:
                hosts.append((host_part, parsed.port))

    return MongoDBURI(
        scheme=parsed.scheme,
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname.split(',')[0] if parsed.hostname else None,
        port=parsed.port,
        database=parsed.path.lstrip('/') if parsed.path else None,
        hosts=hosts,
        extra_params=extra_params
    )


def build_uri(
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        hosts: list[tuple[str, int | None]] | None = None,
        scheme: str = "mongodb",
        **params
) -> str:
    """
    Build MongoDB URI from components
    
    Args:
        user: Username
        password: Password
        host: Hostname (single host)
        port: Port number
        database: Database name
        hosts: List of (host, port) tuples for replica sets
        scheme: URI scheme (mongodb or mongodb+srv)
        **params: Additional query parameters
    """
    # Build netloc
    netloc_parts = []
    if user:
        if password:
            netloc_parts.append(f"{user}:{password}")
        else:
            netloc_parts.append(user)
    
    # Handle multiple hosts (replica set) or single host
    if hosts:
        host_parts = []
        for h, p in hosts:
            if p:
                host_parts.append(f"{h}:{p}")
            else:
                host_parts.append(h)
        netloc_parts.append(",".join(host_parts))
    elif host:
        host_part = host
        if port:
            host_part = f"{host}:{port}"
        netloc_parts.append(host_part)
    
    netloc = "@".join(netloc_parts) if netloc_parts else ""
    
    # Build path
    path = f"/{database}" if database else ""
    
    # Build query
    query = urlencode(params) if params else ""
    
    return urlunparse((scheme, netloc, path, "", query, ""))


def is_uri(dsn: str) -> bool:
    """Check if a string is a MongoDB URI"""
    return dsn.startswith(("mongodb://", "mongodb+srv://"))


def uri_to_dsn(uri: str) -> str:
    """Convert URI to DSN string"""
    parsed = parse_uri(uri)
    return parsed.to_dsn()

