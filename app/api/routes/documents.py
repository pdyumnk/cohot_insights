from typing import Annotated

from fastapi import APIRouter, Path, Response, status

from app.api.deps import CurrentUser, Documents
from app.schemas.documents import (
    ClientDocRef,
    DocumentCreate,
    DocumentCreated,
    DocumentOut,
    DocumentUpdate,
    ErrorOut,
    to_document_out,
)

router = APIRouter(prefix="/documents", tags=["documents"])

NOT_FOUND = {404: {"model": ErrorOut, "description": "No such document for this caller"}}
RATE_LIMITED = {429: {"model": ErrorOut, "description": "Caller already has the maximum number of active documents"}}


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DocumentCreated,
    responses={
        200: {"model": DocumentCreated, "description": "Idempotent replay: same client_doc_ref, same content"},
        403: {"model": ErrorOut, "description": "Body user_id differs from the caller"},
        409: {"model": ErrorOut, "description": "client_doc_ref already registered with different content / owner"},
        **RATE_LIMITED,
    },
)
async def submit_document(body: DocumentCreate, response: Response, caller: CurrentUser, service: Documents):
    outcome = await service.submit(body, caller)
    if not outcome.created:
        response.status_code = status.HTTP_200_OK
    doc = outcome.document
    return DocumentCreated(
        document_id=str(doc["_id"]),
        status=doc["status"],
        content_version=doc["content_version"],
        client_doc_ref=doc.get("client_doc_ref"),
    )


# Declared before /{document_id} so "by-ref" is never parsed as a document id.
@router.get("/by-ref/{client_doc_ref}", response_model=DocumentOut, responses=NOT_FOUND)
async def get_document_by_ref(
    client_doc_ref: Annotated[ClientDocRef, Path()], caller: CurrentUser, service: Documents
):
    return to_document_out(await service.get_by_ref(client_doc_ref, caller))


@router.get("/{document_id}", response_model=DocumentOut, responses=NOT_FOUND)
async def get_document(document_id: str, caller: CurrentUser, service: Documents):
    return to_document_out(await service.get(document_id, caller))


@router.patch(
    "/{document_id}",
    response_model=DocumentOut,
    responses={**NOT_FOUND, 409: {"model": ErrorOut, "description": "content_version conflict"}, **RATE_LIMITED},
)
async def update_document(document_id: str, body: DocumentUpdate, caller: CurrentUser, service: Documents):
    return to_document_out(await service.update_content(document_id, body, caller))


@router.post(
    "/{document_id}/retry",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=DocumentOut,
    responses={**NOT_FOUND, 409: {"model": ErrorOut, "description": "Document is not failed"}, **RATE_LIMITED},
)
async def retry_document(document_id: str, caller: CurrentUser, service: Documents):
    """Resume a failed document at the stage that failed (a failed enriching keeps its summary)."""
    return to_document_out(await service.retry(document_id, caller))
