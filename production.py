import os
import re
import requests
import autogen
from dotenv import load_dotenv


# ─────────────────────────────────────────────
# OUTPUT FOLDER SETUP
# ─────────────────────────────────────────────
def get_output_dir() -> str:
    """Returns path to /samples folder next to main.py, creates it if missing."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "samples")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


load_dotenv()
euri_key = os.getenv("EURI_API_KEY")


class EuronClient:
    def __init__(self, config, **kwargs):
        self.api_key = config["api_key"]
        self.model_name = config["model"]
        self.max_out_tokens = config.get("max_tokens", 1200)
        self.endpoint = "https://api.euron.one/api/v1/euri/chat/completions"

    def create(self, params):
        messages = params.get("messages", [])
        limited_messages = messages[-5:]
        formatted_messages = [
            {"role": m.get("role", "user"), "content": m.get("content", "")}
            for m in limited_messages
        ]
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model_name,
            "messages": formatted_messages,
            "max_tokens": self.max_out_tokens,
            "temperature": 0,
        }
        response = requests.post(self.endpoint, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]

        class MockResponse:
            def __init__(self, content, model):
                self.choices = [self.Choice(content)]
                self.model = model
                self.usage = type(
                    "obj",
                    (object,),
                    {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                )

            class Choice:
                def __init__(self, content):
                    self.message = type(
                        "obj", (object,), {"content": content, "function_call": None}
                    )
                    self.finish_reason = "stop"

        return MockResponse(content, self.model_name)

    def message_retrieval(self, response):
        return [choice.message.content for choice in response.choices]

    def cost(self, response):
        return 0.0

    @staticmethod
    def get_usage(response):
        return {}


def extract_code_from_chat(chat_result, agent_name: str) -> str:
    parts = []
    for msg in chat_result.chat_history:
        msg_name = msg.get("name", "")
        content = (msg.get("content") or "").strip()
        if msg_name == agent_name and content:
            parts.append(content)

    if not parts:
        return ""

    stitched = "\n".join(parts)
    blocks = re.findall(r"```(?:python|pyspark)?\s*\n?(.*?)```", stitched, re.DOTALL)
    if blocks:
        return "\n\n".join(b.strip() for b in blocks)
    return stitched


def extract_text_from_chat(chat_result, agent_name: str) -> str:
    parts = []
    for msg in chat_result.chat_history:
        if msg.get("name", "") == agent_name:
            content = (msg.get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def local_quality_check(code: str) -> tuple[bool, list[str]]:
    failures = []
    if "partitionBy" not in code:
        failures.append(
            "[RULE 1] FAIL — 'partitionBy' not found. Add .write.partitionBy('event_date')"
        )
    if "to_date" not in code:
        failures.append(
            "[RULE 2] FAIL — 'to_date' not found. Derive event_date using to_date(col('event_timestamp'))"
        )
    if "StructType" not in code and "StructField" not in code:
        failures.append(
            "[RULE 3] FAIL — No explicit schema. Define schema using StructType/StructField"
        )
    return len(failures) == 0, failures


def save_conversation(chat_history: list, task_message: str, suffix: str = ""):
    slug = task_message.lower()
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "_", slug.strip())
    slug = slug[:40]
    filename = f"{slug}{suffix}.txt"

    output_path = os.path.join(get_output_dir(), filename)  # <-- changed

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"TASK: {task_message}\n")
        f.write("=" * 80 + "\n\n")
        for message in chat_history:
            role = message.get("role", "unknown").upper()
            name = message.get("name", "")
            label = f"[{role}]" if not name else f"[{role} — {name.upper()}]"
            content = message.get("content", "") or ""
            f.write(f"{label}\n")
            f.write(content.strip())
            f.write("\n\n" + "-" * 80 + "\n\n")

    print(f"✅ Saved: {output_path}")
    return output_path


def make_llm_config(max_tokens: int = 1000):
    return {
        "config_list": [
            {
                "model": "qwen/qwen3-32b",
                "api_key": euri_key,
                "model_client_cls": "EuronClient",
                "max_tokens": max_tokens,
            }
        ],
        "temperature": 0,
    }


architect = autogen.AssistantAgent(
    name="Data_Architect",
    llm_config=make_llm_config(max_tokens=1200),
    system_message="""You are a Senior Data Engineer.

    Every script you write MUST contain ALL THREE of the following:
    [RULE 1] .write.partitionBy('event_date') when saving output
    [RULE 2] to_date(col('event_timestamp')) to derive event_date
    [RULE 3] StructType and StructField for explicit schema definition

    Output the COMPLETE script inside ONE single ```python ... ``` block.""",
)
architect.register_model_client(model_client_cls=EuronClient)

cloud_architect = autogen.AssistantAgent(
    name="Cloud_Architect",
    llm_config=make_llm_config(max_tokens=1200),
    system_message="""You are a Senior Cloud Architect for AWS and Azure.

    Given an approved PySpark script, output TWO clearly labelled sections:

    --- SECTION 1: AWS Glue (Terraform) ---
    - aws_iam_role with glue.amazonaws.com trust policy
    - aws_glue_job: worker_type G.2X, number_of_workers 10, glue_version 4.0

    --- SECTION 2: Azure Databricks (YAML Pipeline) ---
    - Azure DevOps pipeline triggering on main branch
    - Cluster: Standard_DS3_v2, spark_version 13.3.x-scala2.12, num_workers 8""",
)
cloud_architect.register_model_client(model_client_cls=EuronClient)

user_proxy = autogen.UserProxyAgent(
    name="Admin",
    human_input_mode="NEVER",
    max_consecutive_auto_reply=0,
    code_execution_config=False,
)


def run_engineering_squad(task: str, max_review_cycles: int = 4):
    all_history = []
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("\n📐 STAGE 1: Architecting Solution...")
    arch_chat = user_proxy.initiate_chat(architect, message=task, max_turns=3)
    current_script = extract_code_from_chat(arch_chat, "Data_Architect")
    all_history += arch_chat.chat_history

    approved = False
    for cycle in range(1, max_review_cycles + 1):
        print(f"🔍 STAGE 2: Automated Validation Cycle {cycle}...")
        passed, failures = local_quality_check(current_script)

        if passed:
            print("✅ Quality Check Passed.")
            approved = True
            break

        if cycle < max_review_cycles:
            rewrite_chat = user_proxy.initiate_chat(
                architect,
                message=(
                    f"Failures detected. Fix these specific issues:\n\n"
                    f"{chr(10).join(failures)}\n\n"
                    f"Output the complete fixed script in ONE ```python ... ``` block."
                ),
                max_turns=3,
            )
            current_script = extract_code_from_chat(rewrite_chat, "Data_Architect")
            all_history += rewrite_chat.chat_history

    print("☁️  STAGE 3: Generating Infrastructure as Code...")
    cloud_chat = user_proxy.initiate_chat(
        cloud_architect,
        message=(
            f"Deployment Target: AWS Glue & Azure Databricks\n\n"
            f"Approved Script:\n```python\n{current_script}\n```"
        ),
        max_turns=2,
    )
    cloud_output = extract_text_from_chat(cloud_chat, "Cloud_Architect")
    all_history += cloud_chat.chat_history

    print("\n💾 Saving outputs...\n")
    save_conversation(all_history, task, suffix="_full_squad")

    infra_path = os.path.join(get_output_dir(), "infra_config.txt")  # <-- changed
    with open(infra_path, "w", encoding="utf-8") as f:
        f.write("# Infrastructure Config — Auto-generated by Cloud_Architect\n")
        f.write(f"# Task: {task}\n")
        f.write("=" * 80 + "\n\n")
        f.write(cloud_output)
    print(f"✅ Infra config saved: {infra_path}")

    script_path = os.path.join(get_output_dir(), "approved_script.py")  # <-- changed
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(f"# Auto-generated by Data_Architect\n")
        f.write(f"# Review Status: {'APPROVED' if approved else 'BEST EFFORT'}\n\n")
        f.write(current_script)
    print(f"✅ Final script saved: {script_path}")


task = "Write a concise PySpark script for 500GB of shipping logs, partitioned by event_date."
run_engineering_squad(task)
