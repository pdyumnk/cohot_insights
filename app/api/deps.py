from typing import Annotated

from fastapi import Depends, Header, Request

from app.container import Container
from app.schemas.documents import UserId
from app.services.documents import DocumentService


def get_container(request: Request) -> Container:
    return request.app.state.container


def get_document_service(container: Annotated[Container, Depends(get_container)]) -> DocumentService:
    return container.documents


def current_user(
    x_user_id: Annotated[
        UserId,
        Header(description="Caller identity. Stand-in for a verified auth token (see README)."),
    ],
) -> str:
    return x_user_id


CurrentUser = Annotated[str, Depends(current_user)]
Documents = Annotated[DocumentService, Depends(get_document_service)]
