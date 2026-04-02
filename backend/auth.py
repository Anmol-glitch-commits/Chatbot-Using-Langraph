import os
from datetime import datetime, timedelta, timezone
from typing import Annotated

import jwt
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from pwdlib import PasswordHash
from pydantic import BaseModel, EmailStr

from database import pool

load_dotenv()

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = os.getenv("JWT_ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = 60


class Token(BaseModel):
    access_token: str
    token_type: str


class User(BaseModel):
    id: int
    email: EmailStr
    created_at: datetime


password_hash = PasswordHash.recommended()

DUMMY_HASH = password_hash.hash("DUMMYPASSWORD")

oauth2_bearer = OAuth2PasswordBearer(tokenUrl="/users/login")


def verify_password(plain_password: str, hashed_password: str):
    return password_hash.verify(plain_password, hashed_password)


def get_password_hash(password: str):
    return password_hash.hash(password)


def get_user_by_email(email: str):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id,email, password_hash, created_at FROM users WHERE email =  %s",
            (email,),
        ).fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "email": row[1],
            "password_hash": row[2],
            "created_at": row[3],
        }


def get_user_by_id(user_id: int):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id,email, created_at FROM users WHERE id = %s",
            (user_id,),
        ).fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "email": row[1],
            "created_at": row[2],
        }


def authenticate_user(email: str, password: str):
    user = get_user_by_email(email)

    if not user:
        verify_password(password, DUMMY_HASH)
        return None

    if not verify_password(password, user["password_hash"]):
        return None

    return user


def create_access_token(data: dict):
    to_encode = data.copy()

    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_bearer)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")

        if user_id is None:
            raise credentials_exception
    except InvalidTokenError:
        raise credentials_exception

    user = get_user_by_id(int(user_id))
    if user is None:
        raise credentials_exception

    return user
