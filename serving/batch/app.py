import os
import duckdb
import chainlit as cl
from groq import Groq

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SCHEMA_PROMPT = """
You are a SQL expert for a GTFS transit batch analytics platform.
Given a user request, output ONLY a valid DuckDB SQL query without markdown blocks based on this schema:

Tables & Views Available:
1. vehicle_status_history(vehicle_id, route_id, latitude, longitude, speed, status, timestamp)
2. route_health_history(route_id, avg_delay_sec, active_buses, status, timestamp)
3. network_health_history(active_routes, total_vehicles, on_time_pct, timestamp)
4. hourly_vehicle_speeds(route_id, hour_bin, avg_speed_kmh, max_speed_kmh)
5. hourly_route_performance(route_id, hour_bin, mean_delay_sec, max_delay_sec)
6. daily_network_reliability(day_bin, avg_on_time_percentage, avg_active_vehicles)

Return raw executable SQL only.
"""

@cl.on_message
async def main(message: cl.Message):
    completion = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": SCHEMA_PROMPT},
            {"role": "user", "content": message.content}
        ],
        temperature=0.1,
    )
    
    sql_query = completion.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
    
    try:
        conn = duckdb.connect(os.getenv("BATCH_QUERY_DB_PATH", "gtfs_batch_read.db"), read_only=True)
        df = conn.execute(sql_query).df()
        conn.close()
        
        if df.empty:
            await cl.Message(content=f"**Generated SQL:**\n`{sql_query}`\n\nNo records found.").send()
        else:
            await cl.Message(
                content=f"**Generated SQL:**\n`{sql_query}`\n\n**Results:**\n" + df.to_markdown(index=False)
            ).send()
    except Exception as e:
        await cl.Message(content=f"**Execution Error:** `{sql_query}`\n\n**Details:** {str(e)}").send()