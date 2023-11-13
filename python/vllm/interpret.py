import os
import json
import logging
from typing import Optional, List, Dict
from pydantic import BaseModel, Field
from fastapi import FastAPI, Depends, HTTPException
from google.cloud import secretmanager
import vertexai
from vertexai.language_models import TextGenerationModel
from fastapi.middleware.cors import CORSMiddleware
from asgiref.sync import sync_to_async
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
VERTEX_PROJECT = os.getenv("VERTEX_PROJECT", "tfmv-371720")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
MODEL_NAME = os.getenv("MODEL_NAME", "text-bison")

# Initialize FastAPI application
app = FastAPI(title="TFMV Interpretation API",
              description="TFMV LLM for text interpretation.",
              version="1.0")

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Enums and Models
class InsightType(str, Enum):
    emotional_intelligence = "emotional_intelligence"
    work_dynamics = "work_dynamics"
    unintended_meanings = "unintended_meanings"
    tone = "tone"
    summary_of_key_observations = "summary_of_key_observations"
    estimated_iq = "estimated_iq"

class InsightToggles(BaseModel):
    emotional_intelligence: bool = Field(True, description="Toggle Emotional Intelligence")
    work_dynamics: bool = Field(True, description="Toggle Work Dynamics")
    unintended_meanings: bool = Field(True, description="Toggle Unintended Meanings")
    tone: bool = Field(True, description="Toggle Tone")
    summary_of_key_observations: bool = Field(True, description="Toggle Summary of Key Observations")
    estimated_iq: bool = Field(True, description="Toggle Estimated IQ")

class Insight(BaseModel):
    insight: str = Field(..., description="Generated insight.")

class InterpretationResponse(BaseModel):
    interpretation: str = Field(..., description="Generated interpretation.")
    insights: Dict[InsightType, List[Insight]] = Field(default_factory=dict, description="Generated insights classified by insight type.")

class InterpretationRequest(BaseModel):
    content: str = Field(..., description="Text content to interpret.")
    enable_insights: bool = Field(default=False, description="Whether to generate insights.")
    insight_toggles: InsightToggles = InsightToggles()

# Secret Manager Access
def access_secret_version(project_id: str, secret_id: str) -> str:
    """
    Access the latest version of a secret in Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

# Model Initialization
def get_model() -> TextGenerationModel:
    """
    Initialize and return the VertexAI Text Generation Model.
    """
    vertexai.init(project=VERTEX_PROJECT, location=VERTEX_LOCATION)
    return TextGenerationModel.from_pretrained(MODEL_NAME)

# Insight Generation
async def generate_single_insight(model: TextGenerationModel, interpretation: str, insight_type: InsightType, prompts: dict) -> Optional[Insight]:
    """
    Generate a single insight based on the provided model, interpretation, insight type, and prompts.
    """
    try:
        logger.info(f"Generating insight for: {insight_type}")
        prompt = prompts.get(insight_type.value)
        if not prompt:
            logger.warning(f"No prompt found for insight type: {insight_type}")
            return None

        response = await sync_to_async(model.predict)(
            prompt.format(content=interpretation),
            temperature=0.35,
            max_output_tokens=1024,
            top_k=40,
            top_p=0.95,
        )
        response_text = response.text.strip()
        return Insight(insight=response_text) if response_text else None
    except Exception as e:
        logger.error(f"Error while generating insight for {insight_type}: {e}")
        return None

async def generate_insights(model: TextGenerationModel, interpretation: str, insight_toggles: InsightToggles) -> Dict[InsightType, List[Insight]]:
    insights = {}
    secret_data = access_secret_version(VERTEX_PROJECT, "insight-prompts")
    prompts = json.loads(secret_data)

    for insight_type in InsightType:
        if getattr(insight_toggles, insight_type.value, False):
            insight = await generate_single_insight(model, interpretation, insight_type, prompts)
            if insight:
                insights.setdefault(insight_type, []).append(insight)
    return insights

# API Endpoints
@app.post("/interpret", response_model=InterpretationResponse)
async def interpret(request: InterpretationRequest, model: TextGenerationModel = Depends(get_model)) -> InterpretationResponse:
    """
    Generate interpretation and insights from the provided text content.
    """
    try:
        response = await sync_to_async(model.predict)(
            request.content,
            temperature=0.35,
            max_output_tokens=1024,
            top_k=40,
            top_p=0.95,
        )
        interpretation = response.text.strip()

        response_insights = await generate_insights(model, interpretation, request.insight_toggles) if request.enable_insights else {}

        return InterpretationResponse(interpretation=interpretation, insights=response_insights)
    except Exception as e:
        logger.error(f"Error in interpretation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
