import aioredis
import asyncio

async def createDatabase():
    try:
        # Connect to Redis using the new aioredis 2.0 syntax
        redis = await aioredis.from_url('redis://localhost', encoding="utf-8", decode_responses=True)
        await redis.flushdb()
        await redis.close()
        print("Database created successfully.")
        return True
    except Exception as e:
        print(f"Error during database operation: {e}")
        return False


if __name__ == "__main__":
    asyncio.run(createDatabase())