"""Repository-wide Python startup customizations."""

from application.services.runtime_service import apply_repository_runtime_limits

apply_repository_runtime_limits()
