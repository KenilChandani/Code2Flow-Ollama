import streamlit as st
import asyncio
from ollama import AsyncClient,pull
from graphviz import Source
import uuid
from pathlib import Path
from PIL import Image
import logging
import aiohttp

MODEL_NAME = "llama3.1"
url = 'http://localhost:11434'

def save_dot_to_image(dot_code: str, output_dir="images") -> str:
    Path(output_dir).mkdir(exist_ok=True)
    file_id = str(uuid.uuid4())
    output_path = Path(output_dir) / f"{file_id}.png"
    
    src = Source(dot_code)
    src.format = "png"
    src.render(filename=file_id, directory=output_dir, cleanup=True)
    
    return str(output_path)

async def pull_model_instance():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # Check if the response status is OK (200-299)
                if response.status in range(200, 300):
                    return True
                else:
                    logging.debug(f" ollama_health : check_service_running : Received status code {response.status} from {url}")
                    return False
    except aiohttp.ClientError as exe:
        logging.error(f" ollama_health : check_service_running : Client error occurred: {exe}")
        return False
    try:
        # Initiates the image pulling process from the oLLaMa_model source
        image_pull_response = pull(MODEL_NAME, stream=True)
        progress_states = set()
        print(f" utility : pull_model_instance : Instansiating '{MODEL_NAME}' ... ")

        # Iterates through the progress updates of the image pull operation
        for progress in image_pull_response:
            # Skips progress updates if the status is already processed
            if progress.get("status") in progress_states:
                continue
            
            # Adds the current status to the set of processed statuses
            progress_states.add(progress.get("status"))
            
            # Logs the current status of the image pull process
            print(f" utility : pull_model_instance : '{MODEL_NAME}' Model Fetching Status : {progress.get('status')}")
    
    except aiohttp.ClientError as exe:
        # Logs an error message if an exception occurs during the image pull operation
        logging.error(f" utility : pull_model_instance : Exception Occurred while Pulling Model Instance: {exe}")

st.set_page_config(page_title="Code to Flowchart", layout="wide")
st.title("🧠 Code → 📈 Flowchart")

code_input = st.text_area("Paste your Python code below:", height=200, placeholder="def foo(x):\n    if x > 0:\n        return 'yes'\n    else:\n        return 'no'")

if st.button("Generate Flowchart"):
    if not code_input.strip():
        st.warning("Please enter some code.")
    else:
        with st.spinner("Generating flowchart..."):
            async def generate_flowchart(code: str) :
                prompt = f"Convert the following Python function into a flowchart in DOT format (Graphviz).Return only the DOT code. Do not include explanations or markdown.\n\n Code:{code}"
                await pull_model_instance()
                client = AsyncClient(host=url)  # No async with
                
                response = await client.chat(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that converts code into flowcharts using DOT format (Graphviz)."},
                        {"role": "user", "content": prompt}
                    ]
                )

                text = response.get("message", {}).get("content", "")
                return text.strip()

            # Run asyncio inside Streamlit
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            dot_code = loop.run_until_complete(generate_flowchart(code_input))
            
            # Save in session_state
            st.session_state["dot_code"] = dot_code
            st.session_state["image_path"] = save_dot_to_image(dot_code)
            
            if "dot_code" in st.session_state and "image_path" in st.session_state:
                st.subheader("📊 Generated Flowchart")
                st.code(st.session_state["dot_code"], language="dot")

                st.subheader("🔍 Preview")
                st.image(Image.open(st.session_state["image_path"]), caption="Flowchart", use_column_width=True)

                with open(st.session_state["image_path"], "rb") as f:
                    st.download_button("Download PNG", f, file_name="flowchart.png")