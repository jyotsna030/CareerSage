from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
import configparser
import jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel, EmailStr
from connections import mongo_connection

router = APIRouter()

config = configparser.ConfigParser()
config.read('./configuration.properties')

# User model
class User(BaseModel):
    username: str
    password: str
    email: EmailStr


# JWT config
SECRET_KEY = config['auth-api']['SECRET_KEY']
ALGORITHM = config['auth-api']['ALGORITHM']
ACCESS_TOKEN_EXPIRE_MINUTES = int(
    config['auth-api']['ACCESS_TOKEN_EXPIRE_MINUTES'])

# Password hashing settings
schemes = config['password']['schemes']
deprecated = config['password']['deprecated']
pwd_context = CryptContext(schemes=schemes, deprecated=deprecated)

# Function to authenticate user
def authenticate_user(username: str, password: str, collection):
    user = collection.find_one({"username": username})
    # Verify the plain password with the hash password from db
    if not user or not pwd_context.verify(password, user["password"]):
        return False
    return user

# Function to create access token
def create_access_token(username: str):
    expiration_time = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {"sub": username, "exp": expiration_time}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


# Route to handle user signup
@router.post("/signup")
async def user_signup(user: User):
    try:
        if not user:
            return False

        user_data = user.dict()

        db = mongo_connection()
        collection = db[config['MONGODB']["COLLECTION_USER"]]
        existing_user = collection.find_one({"email": user_data["email"]})
        if existing_user:
            raise HTTPException(status_code=400, detail="User with the same email already exists") 
        
        last_user = collection.find_one(sort=[("userid", -1)])
        new_user_id = last_user["userid"] + 1 if last_user else 1
        user_data["userid"] = new_user_id
        
        # Hash the password
        user_data["password"] = pwd_context.hash(user_data["password"])
        result = collection.insert_one(user_data)

        if result.inserted_id:
            return {
                "username": user_data["username"],
                "email": user_data["email"],
                "userid": user_data["userid"]
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create user")

    except HTTPException as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as e:
        print("An error occurred:", e)
        raise HTTPException(status_code=500, detail="Internal server error")


# Route to generate access token after login
@router.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    db = mongo_connection()
    collection = db[config['MONGODB']["COLLECTION_USER"]]
    user = authenticate_user(
        form_data.username, form_data.password, collection)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(form_data.username)
    if access_token:
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        return {"message": "Failed"}
