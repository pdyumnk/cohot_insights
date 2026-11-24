from typing import Annotated

from fastapi import APIRouter, Path, Query

from app.api.deps import CurrentUser, Documents
from app.domain import Status
from app.schemas.documents import DocumentPage, ErrorOut, UserId, to_list_item

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "/{user_id}/documents",
    response_model=DocumentPage,
    responses={404: {"model": ErrorOut, "description": "user_id is not the caller"}},
)
async def list_user_documents(
    user_id: Annotated[UserId, Path()],
    caller: CurrentUser,
    service: Documents,
    page: Annotated[int, Query(ge=1, le=10_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status: Annotated[Status | None, Query()] = None,
):
    """Newest first. Only the caller's own documents; another user's listing is a 404."""
    docs, has_more = await service.list_for_user(user_id, caller, page, page_size, status)
    return DocumentPage(items=[to_list_item(d) for d in docs], page=page, page_size=page_size, has_more=has_more)
