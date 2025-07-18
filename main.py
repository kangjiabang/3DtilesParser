import uvicorn


def main():
    uvicorn.run(
        "src.api.web:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    main()