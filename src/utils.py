import os
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger('utils')

def sanitize_ip_info(ip_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize IP information before saving to database.
    Handle nested structures and ensure all fields have proper types.
    """
    sanitized = {}
    
    # Basic string fields with defaults
    string_fields = [
        'ip', 'hostname', 'city', 'region', 'country', 
        'loc', 'org', 'postal', 'timezone', 'asn'
    ]
    
    for field in string_fields:
        sanitized[field] = str(ip_info.get(field, '')) if ip_info.get(field) is not None else ''
    
    # Handle nested structures
    # Company
    if isinstance(ip_info.get('company'), dict):
        sanitized['company'] = ip_info['company'].get('name', '')
    else:
        sanitized['company'] = ''
        
    # Carrier
    if isinstance(ip_info.get('carrier'), dict):
        sanitized['carrier'] = ip_info['carrier'].get('name', '')
    else:
        sanitized['carrier'] = ''
    
    # Abuse contact
    if isinstance(ip_info.get('abuse'), dict):
        sanitized['abuse_email'] = ip_info['abuse'].get('email', '')
        sanitized['abuse_phone'] = ip_info['abuse'].get('phone', '')
    else:
        sanitized['abuse_email'] = ''
        sanitized['abuse_phone'] = ''
    
    # Boolean fields
    sanitized['is_bogon'] = bool(ip_info.get('bogon', False))
    sanitized['is_mobile'] = bool(ip_info.get('mobile', False))
    
    return sanitized