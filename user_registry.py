"""
User registry: channel-agnostic canonical name resolution.

Loads ~/.hermes/users.yaml and provides lookups in both
directions (canonical name → platform IDs, platform ID → canonical name).
"""

import os
import threading
from typing import Dict, List, Optional
import yaml

_REGISTRY: Optional[Dict] = None
_REGISTRY_LOCK = threading.Lock()
_RAW_DATA: Optional[Dict] = None


def _load_registry() -> Dict:
    global _REGISTRY, _RAW_DATA
    if _REGISTRY is None:
        with _REGISTRY_LOCK:
            if _REGISTRY is None:
                path = os.path.expanduser("~/.hermes/users.yaml")
                if os.path.exists(path):
                    with open(path) as f:
                        data = yaml.safe_load(f) or {}
                else:
                    data = {}
                _RAW_DATA = data
                _REGISTRY = data.get("users", {})
    return _REGISTRY


def reload_registry() -> None:
    """Force re-read from disk (e.g. after edits)."""
    global _REGISTRY, _RAW_DATA
    with _REGISTRY_LOCK:
        _REGISTRY = None
        _RAW_DATA = None
    _load_registry()


def resolve_canonical_name(
    user_id: Optional[str] = None,
    platform: Optional[str] = None,
) -> Optional[str]:
    """Resolve a platform user_id to canonical name (e.g. 'alice')."""
    if not user_id:
        return None
    registry = _load_registry()
    for canonical, info in registry.items():
        if not isinstance(info, dict):
            continue
        # Direct match on platform fields
        for plat_key, plat_val in info.items():
            if plat_key == "display_name":
                continue
            if platform and plat_key != platform:
                continue
            if str(plat_val) == str(user_id):
                return canonical
        # Also match if canonical name itself is the user_id (legacy)
        if canonical == str(user_id):
            return canonical
    return None


def resolve_user_id(
    canonical_name: str,
    platform: str = "telegram",
) -> Optional[str]:
    """Get a platform-specific user_id for a canonical name."""
    registry = _load_registry()
    info = registry.get(canonical_name)
    if not isinstance(info, dict):
        return None
    return info.get(platform)


def all_canonical_names() -> List[str]:
    """Return all registered canonical names."""
    return list(_load_registry().keys())


def get_display_name(canonical_name: str) -> Optional[str]:
    """Human-readable display name for a canonical user."""
    registry = _load_registry()
    info = registry.get(canonical_name)
    if isinstance(info, dict):
        return info.get("display_name")
    return None


def get_cli_default() -> Optional[str]:
    """Canonical name to use for CLI sessions."""
    _load_registry()
    if _RAW_DATA is not None:
        return _RAW_DATA.get("cli_default")
    return None


def resolve_for_store(user_id: Optional[str], platform: Optional[str] = None) -> str:
    """
    Return canonical name for DB filename, or 'default' if none found.
    Used by holographic memory store: memory_store_{canonical}.db
    """
    name = resolve_canonical_name(user_id, platform)
    return name or "default"


def get_guardians(child_name: str) -> List[str]:
    """Return list of canonical guardian names for a child, or []."""
    registry = _load_registry()
    info = registry.get(child_name)
    if isinstance(info, dict):
        return info.get("guardians", [])
    return []


def get_dependents(guardian_name: str) -> List[str]:
    """Return list of canonical child names this guardian is responsible for."""
    registry = _load_registry()
    dependents = []
    for canonical, info in registry.items():
        if isinstance(info, dict) and guardian_name in info.get("guardians", []):
            dependents.append(canonical)
    return dependents


def get_visible_names(user_id: Optional[str] = None, platform: Optional[str] = None) -> List[str]:
    """
    Return list of canonical names whose context this user may view.
    For guardians, includes their own name + all dependent children.
    For children, returns only their own name.
    For unknown users, returns ['default'].
    """
    name = resolve_canonical_name(user_id, platform)
    if not name:
        return ["default"]
    visible = [name]
    # Guardians can view dependents
    dependents = get_dependents(name)
    visible.extend(dependents)
    return visible


def get_user_scope(canonical_name: str) -> Optional[str]:
    """Return 'adult' or 'child' for a canonical user, or None if unknown."""
    registry = _load_registry()
    info = registry.get(canonical_name)
    if isinstance(info, dict):
        return info.get("scope")
    return None


def get_user_scope_by_id(user_id: str, platform: Optional[str] = None) -> Optional[str]:
    """Convenience: resolve user_id to canonical name, then return scope."""
    name = resolve_canonical_name(user_id, platform)
    if name:
        return get_user_scope(name)
    return None
