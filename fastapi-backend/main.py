from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import authRoute, userRoutes, analyticsRoute

app = FastAPI()

app.include_router(authRoute.router, tags=['authRoute'], prefix='')
app.include_router(userRoutes.router, tags=['userRoutes'], prefix='/userRoutes')
app.include_router(analyticsRoute.router, tags=['analyticsRoute'], prefix='/analyticsRoute')