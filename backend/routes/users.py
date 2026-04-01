from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from auth import (
    get_user_by_email,
    get_password_hash,
    authenticate_user,
    create_access_token,
)
from database import create_user, get_all_users

router = APIRouter(prefix="/users", tags=["USER endpoints"])


class Request(BaseModel):
    email: str
    password: str


@router.get("/")
async def list_users():
    users = get_all_users()
    return users


@router.post("/register")
async def register(req: Request):
    req_email = req.email.strip().lower()

    if not req_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email cannot be empty",
        )

    existing_user = get_user_by_email(req_email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        )

    hashed_password = get_password_hash(req.password)
    user = create_user(req_email, hashed_password)

    return {
        "message": "User registered successfully",
        "user": user,
    }


@router.post("/login")
async def login(req: Request):
    req_email = req.email.strip().lower()

    user = authenticate_user(req_email, req.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    access_token = create_access_token({"sub": str(user["id"])})

    return {"access_token": access_token, "token_type": "bearer"}
