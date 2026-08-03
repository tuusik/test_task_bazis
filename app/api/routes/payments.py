from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import get_payment_service
from app.core.exceptions import IdempotencyConflictError
from app.schemas.payment import SPaymentCreate, SPaymentDetail, SPaymentResponse
from app.services.payments import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])


@router.get(
    "/{payment_id}", response_model=SPaymentDetail, status_code=status.HTTP_200_OK
)
async def get_payment(
    payment_id: UUID, service: PaymentService = Depends(get_payment_service)
):
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )
    return SPaymentDetail.model_validate(payment)


@router.post("", response_model=SPaymentResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    payload: SPaymentCreate,
    idempotency_key: str = Header(
        alias="Idempotency-Key", min_length=1, max_length=255
    ),
    service: PaymentService = Depends(get_payment_service),
):

    idempotency_key = idempotency_key.strip()
    if not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key must not be blank",
        )

    try:
        payment = await service.create_payment(payload, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different request",
        ) from exc

    return SPaymentResponse.model_validate(payment)
