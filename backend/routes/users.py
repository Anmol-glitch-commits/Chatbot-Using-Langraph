from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr

from auth import (
    get_user_by_email,
    get_password_hash,
    authenticate_user,
    create_access_token,
    get_current_user,
)
from database import create_user, delete_user

router = APIRouter(prefix="/users", tags=["USER endpoints"])


class Request(BaseModel):
    email: EmailStr
    password: str


class User(BaseModel):
    id: int
    email: EmailStr
    created_at: datetime


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
async def login(req: Annotated[OAuth2PasswordRequestForm, Depends()]):
    req_email = req.username.strip().lower()
    user = authenticate_user(req_email, req.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    access_token = create_access_token({"sub": str(user["id"])})

    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=User)
async def get_me(current_user: Annotated[dict, Depends(get_current_user)]):
    return current_user


@router.delete("/{user_id}")
async def delete_user_by_id(
    user: Annotated[dict, Depends(get_current_user)], user_id: int
):
    if user["id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You cannot delete another user",
        )
    deleted = delete_user(user_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return {"message": "User deleted successfully"}
