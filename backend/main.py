from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import lifespan
from app.routers import finance, genres, movies, overview, people, ratings, temporal

app = FastAPI(
    title="Big Movies API",
    description="REST API for movie analytics data warehouse",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(finance.router, prefix="/api")
app.include_router(overview.router, prefix="/api")
app.include_router(ratings.router, prefix="/api")
app.include_router(genres.router, prefix="/api")
app.include_router(people.router, prefix="/api")
app.include_router(temporal.router, prefix="/api")
app.include_router(movies.router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "Big Movies API", "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
