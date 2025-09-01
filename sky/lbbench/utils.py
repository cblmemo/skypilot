"""Common utilities for the load balancer benchmark."""

import dataclasses
from typing import Any, Dict, List, Optional

import sky

OAIChatHistory = List[Dict[str, str]]

# sgl_cluster = 'sgl-router'
# sky_sgl_enhanced_cluster = 'sgl-router-pull'
# vanilla_least_load_cluster = 'vanilla-least-load'
# global_least_load_cluster = 'global-least-load'
# consistent_hashing_cluster = 'consistent-hashing'
# consistent_hashing_enhanced_cluster = 'consistent-hashing-enhanced'

single_lb_clusters = [
    'sgl-router',
    'least-load',
    'consistent-hashing',
    'round-robin',
]


def _get_sp_c_envs(max_concurrent_requests: int) -> Dict[str, str]:
    return {
        'FORCE_DISABLE_STEALING': 'true',
        'USE_IE_QUEUE_INDICATOR': 'false',
        'DO_PUSHING_TO_REPLICA': 'true',
        'MAX_CONCURRENT_REQUESTS': str(max_concurrent_requests),
    }


single_lb_policy_and_extra_args = [
    (None, None),
    ('least_load', None),
    ('consistent_hashing', None),
    ('round_robin', None),
]


def sky_serve_status() -> List[Dict[str, Any]]:
    req = sky.serve.status(None)
    return sky.client.sdk.get(req)


def sky_status() -> List[Dict[str, Any]]:
    req = sky.status()
    return sky.client.sdk.get(req)


@dataclasses.dataclass
class Metric:
    """Metric for each request."""
    uid: str
    start: Optional[float] = None
    response_start: Optional[float] = None
    streaming_start: Optional[float] = None
    end: Optional[float] = None
    ttft: Optional[float] = None
    e2e_latency: Optional[float] = None
    failed: Optional[str] = None
    input_tokens: Optional[int] = None
    output_tokens: int = 0
    headers: Optional[Dict[str, str]] = None
    cached_tokens: Optional[int] = None
    hash_key: Optional[str] = None
    program_id: Optional[str] = None


def get_one_round(role: str, content: str) -> Dict[str, str]:
    return {'role': role, 'content': content}
