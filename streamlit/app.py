import os
import json

import psycopg2
import chromadb

from openai import OpenAI
import streamlit as st


def document_tool(query_keywords, vehicle, year):
    client = chromadb.HttpClient(
        host=os.getenv("CHROMA_HOST"), 
        port=os.getenv("CHROMA_PORT")
    )
    collection = client.get_collection(
        name="vehicle-docs"
    )
    
    metadata = {}

    if vehicle:
        metadata["vehicle"] = vehicle

    if year:
        metadata["year"] = year

    if metadata:
        if metadata.get("vehicle") and metadata.get("year"):
            result = collection.query(
                query_texts=query_keywords,
                where={
                    "$and": [
                        {"vehicle": vehicle},
                        {"year": year}
                    ]
                }
            )
        elif metadata.get("vehicle"):
            result = collection.query(
                query_texts=query_keywords,
                where={"vehicle": vehicle}
            )
        elif metadata.get("year"):
            result = collection.query(
                query_texts=query_keywords,
                where={"year": year}
            )
    else:
        result = collection.query(
            query_texts=query_keywords
        )

    files = ", ".join(
        set(
            map(
                lambda x: x.get("filename"), 
                result.get("metadatas")[0]
            )
        )
    )
    vehicles = ", ".join(
        set(
            map(
                lambda x: x.get("vehicle"), 
                result.get("metadatas")[0]
            )
        )
    )
    models = ", ".join(
        set(
            map(
                lambda x: x.get("model"), 
                result.get("metadatas")[0]
            )
        )
    )
    pages = ", ".join(
        set(
            map(
                lambda x: x.get("page"), 
                result.get("metadatas")[0]
            )
        )
    )
    document = ", ".join(result.get("documents")[0])

    return json.dumps(
        {
            "documents": document,
            "metadata": {
                "files": files,
                "vehicles": vehicles,
                "models": models,
                "pages": pages
            }
        }
    )

def sql_tool(query):
    connection = psycopg2.connect(
        database=os.getenv("POSTGRES_DB"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"), 
        host=os.getenv("POSTGRES_HOST"), 
        port=5432
    )

    cursor = connection.cursor()

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [_[0] for _ in cursor.description]

    return json.dumps(
        {
            "query": query,
            "response": [columns, records]
        }
    )

def get_dim_categories():
    connection = psycopg2.connect(
        database=os.getenv("POSTGRES_DB"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"), 
        host=os.getenv("POSTGRES_HOST"), 
        port=5432
    )

    category_map = {}

    cursor = connection.cursor()

    # Country
    cursor.execute("SELECT DISTINCT country FROM dim_country;")
    records = cursor.fetchall()

    category_map["country"] = [_[0] for _ in records]

    # Country Code
    cursor.execute("SELECT DISTINCT country_code FROM dim_country;")
    records = cursor.fetchall()

    category_map["country_code"] = [_[0] for _ in records]

    # Region
    cursor.execute("SELECT DISTINCT region FROM dim_country;")
    records = cursor.fetchall()

    category_map["region"] = [_[0] for _ in records]

    # Model Name
    cursor.execute("SELECT DISTINCT model_name FROM dim_model;")
    records = cursor.fetchall()

    category_map["model_name"] = [_[0] for _ in records]

    # Brand
    cursor.execute("SELECT DISTINCT brand FROM dim_model;")
    records = cursor.fetchall()

    category_map["brand"] = [_[0] for _ in records]

    # Segment
    cursor.execute("SELECT DISTINCT segment FROM dim_model;")
    records = cursor.fetchall()

    category_map["segment"] = [_[0] for _ in records]

    # Powertrain
    cursor.execute("SELECT DISTINCT powertrain FROM dim_model;")
    records = cursor.fetchall()

    category_map["powertrain"] = [_[0] for _ in records]

    # Ordertype Name
    cursor.execute("SELECT DISTINCT ordertype_name FROM dim_ordertype;")
    records = cursor.fetchall()

    category_map["ordertype_name"] = [_[0] for _ in records]

    # Powertrain
    cursor.execute("SELECT DISTINCT description FROM dim_ordertype;")
    records = cursor.fetchall()

    category_map["description"] = [_[0] for _ in records]

    return category_map

def parse_run(run):
    if run.status == "completed":
        pass
    elif run.status == "requires_action":
        tool_outputs = []
        
        for tool in run.required_action.submit_tool_outputs.tool_calls:
            if tool.function.name == "sql_tool":
                tool_outputs.append({
                    "tool": tool.function.name,
                    "output": sql_tool(
                        json.loads(tool.function.arguments).get("query")
                    )
                })
            elif tool.function.name == "document_tool":
                tool_outputs.append({
                    "tool": tool.function.name,
                    "output": document_tool(
                        json.loads(tool.function.arguments).get("query"),
                        json.loads(tool.function.arguments).get("vehicle"),
                        json.loads(tool.function.arguments).get("year")
                    )
                })

        try:
            run = client.beta.threads.runs.submit_tool_outputs_and_poll(
                thread_id=thread.id,
                run_id=run.id,
                tool_outputs=tool_outputs
            )
        except Exception as e:
            raise ValueError("Failed to submit tool outputs:", e)
        
    message_results = client.beta.threads.messages.list(
        thread_id=thread.id
    )

    response = message_results.data[0].content[0].text.value

    return response, tool_outputs


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "sql_tool",
            "description": "Use this function to fetch data from a Postgres database when a user asks questions about vehicle sales, time, country/region, model, or powertrain",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": """
                        SQL query to extract data from a Postgres database
                        """
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "document_tool",
            "description": "Use this function to fetch data from the Chromadb collection when a user asks questions about warranty terms, policy clauses, or owner’s manual content",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": """
                        The query to use to extract data from a chromadb collection
                        """
                    },
                    "vehicle": {
                        "type": "string",
                        "description": """
                        The vehicle stated in the users query if any
                        """
                    },
                    "year": {
                        "type": "number",
                        "description": """
                        The specific year the user is requesting information about
                        """
                    }
                },
                "required": ["query"]
            }
        }
    },
]

PROMPT = f"""
You are an assistant that provides information to customers about vehicles based on data stored in your organizations database or a library of vehicle contracts, manuals or warranty's. 

Your goal is to answer the user questions as accurately as possible by following the following steps: 

Step 1: Warm introduction 
Gently greet the user and ask them how you may help them 

Step 2: Information Gathering 
Based on the user's question or request, choose which substeps to run based on the instructions below: 

    - For questions about vehicle sales, time, country/region, model, or powertrain, move to step 3. 
    - For questions about warranty terms, policy clauses, or owner’s manual content, move to step 4. 
    - For questions about vehicle sales, time, country/region, model, or powertrain and warranty terms, policy clauses, or owner’s manual content move to step 5. 
    
Step 3: SQL-related questions 
Provide an SQL query to the sql_tool to return the requested data from a Postgreql database. 
The query should be optimised to return the exact information the user requests for. 
Where possible use CTE over sub queries and join tables where needed.

CREATE TABLE dim_country ( 
    country VARCHAR(100) UNIQUE NOT NULL, 
    country_code VARCHAR(2) PRIMARY KEY, 
    region VARCHAR(100) NOT NULL 
); 
CREATE TABLE dim_model ( 
    model_id INT PRIMARY KEY, 
    model_name VARCHAR(20) NOT NULL, 
    brand VARCHAR(20) NOT NULL, 
    segment VARCHAR(20), 
    powertrain VARCHAR(20) 
); 
CREATE TABLE dim_ordertype ( 
    ordertype_id INT PRIMARY KEY, 
    ordertype_name VARCHAR(10) UNIQUE NOT NULL, 
    "description" VARCHAR(60) NOT NULL 
); 
CREATE TABLE fact_sales_ordertype ( 
    model_id INT REFERENCES dim_model (model_id) NOT NULL, 
    country_code VARCHAR(2) REFERENCES dim_country (country_code) NOT NULL, 
    "year" VARCHAR(4) NOT NULL, 
    "month" INT NOT NULL, 
    contracts INT NOT NULL, 
    ordertype_id INT REFERENCES dim_ordertype (ordertype_id) NOT NULL 
); 
CREATE TABLE fact_sales ( 
    model_id INT REFERENCES dim_model (model_id) NOT NULL, 
    country_code VARCHAR(2) REFERENCES dim_country (country_code) NOT NULL, 
    "year" VARCHAR(4) NOT NULL, 
    "month" INT NOT NULL, 
    contracts INT NOT NULL 
); 

Find below an object with the column names and available values from the categorical values in the tables above to help with writing out your queries:
{json.dumps(get_dim_categories())}


NOTE THAT YOU ARE REQUIRED TO USE THE EXACT TABLES AND COLUMN NAMES TO BUILD AN SQL QUERY FOR A POSTGRESQL DATABASE BASED ON THE SCHEMA FROM THE CREATE TABLE QUERIES ABOVE. 

If you were not brought to this step from step 5, move to step 6. 

Step 4: Document-related questions 
For these types of questions or tasks, note that the data being requested can be retrieved from a chromadb collection, thus provide the necessary query to the document_tool to retrieve that data. For a query that requires multiple different facts, provide a comma separated list of queries to satisfy each request.
The chromadb collection includes meta data such as vehicle, year, page and filename thus you can include that in the functions input to help retrieve the right information. 
After filtering the chromdadb collection, retrieve an answer for the users question based on the documents retrieved ONLY.
If you were not brought to this step from step 5, move to step 6. 

Step 5: SQL & Document-related questions 
For queries that fall to this step, go to step 3 AND step 4 then follow those exact instructions. 
Feel free to combine facts or information from step 3 and step 4 at this point as they are relatable but remember to only base and facts on what has being retrieved from the previous steps.
Afterwards, move to step 6. 

Step 6: Results 
After retrieving the required data from the required database based on the tools provided, provide the answer to the user in text format by rewriting the results in the object provided in a readable and easy-to-understand format. 

For answers from the SQL tool (Step 5), the answers are returned with the query and the response object containing the columns and values of a table. Give the user the resulting answer in a tabular form and also provide the query used to achieve that result.
For answers from the Document tool (Step 4), the answers are returned with the documents to extract the exact answers from and some metadata to present as your reference source. Let the user know the files and vehicles the information was extracted from in addition to the answer based on the files retrieved. The goal is to let the user know exactly where they can find the piece of information you just extracted so feel free to provide as much details as possible to help with this based on the information extracted.

Provide the results from the tools used in a conversational manner that the user would understand without modifying the original content.

Additionally, remember to provide the SQL query used to generate the results being displayed (where applicable) and the details of the files where the information can be located (where applicable).

Step 7: Wraping Up 
Thank the user for their trust and confirm if you correctly answered their questions. 
If the user confirms that their question has been correctly answered end the conversation, otherwise move back to step 1 and start over. 
Likewise, if the user has another question, move back to step 1. 


Run through the provided steps in the background without letting the user know which step is being processed. 
Only return the results in a conversational format. 
Remember that your goal is the answer the users questions as accurately as possible.

NOTE: Always provide your reference for where or how you retrieved your information whether it's in the form of an SQL query, a document name, a file name, or all of the previous options!!
"""

INSTRUCTION = """
You are an assistant that provides information to customers about vehicles based on data stored in your organizations database or a library of vehicle contracts, manuals or warranty's. 

Your goal is to answer the user questions as accurately as possible by retrieving data from various databases based on the tools available to the conversation thread.

After retrieving the required data from the required database based on the tools provided, provide the answer to the user in text format by rewriting the results in the object provided in a readable and easy-to-understand format. 
"""

st.title("HazelHeartwood Vehicle Assistant")

client = OpenAI()

assistant = client.beta.assistants.create(
    name="Customer Service Assitant",
    instructions=INSTRUCTION,
    model="gpt-4o-mini",
    temperature=0.6
)

thread = client.beta.threads.create()
message = client.beta.threads.messages.create(
    thread_id=thread.id,
    role="assistant",
    content=PROMPT
)

run = client.beta.threads.runs.create_and_poll(
    thread_id=thread.id,
    assistant_id=assistant.id,
    instructions="Use a helpful and calm tone with the user",
    tools=TOOLS
)

convo_start = parse_run(run)

if "messages" not in st.session_state:
    st.session_state.messages = []

with st.chat_message("assistant"):
    st.write(convo_start)

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Enter your query:"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)

    message = client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=prompt
    )
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread.id,
        assistant_id=assistant.id,
        instructions="Use a helpful and calm tone with the user",
        tools=TOOLS
    )

    try:
        response, tools = parse_run(run)

        with st.chat_message("assistant"):
            st.write(f"The following tools were used: {json.dumps(tools)}")

        st.session_state.messages.append({"role": "assistant", "content": json.dumps(tools)})

        with st.chat_message("assistant"):
            st.write(response)

        st.session_state.messages.append({"role": "assistant", "content": response})
    except:
        client.beta.threads.messages.delete(
            message_id=message.id,
            thread_id=thread.id
        )

        with st.chat_message("assistant"):
            st.write("There waas an error while processing your request. Please try again")

        st.session_state.messages.append({"role": "assistant", "content": "There waas an error while processing your request. Please try again"}) 

    