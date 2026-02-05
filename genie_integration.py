"""
Databricks Genie Integration
Provides chatbot interface using Genie Spaces/Rooms
"""

from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient
from auth_middleware import UserContext
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class GenieService:
    """Service for interacting with Databricks Genie"""
    
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        default_space_id: Optional[str] = None
    ):
        self.client = workspace_client
        self.default_space_id = default_space_id
    
    async def ask_question(
        self,
        question: str,
        space_id: Optional[str] = None,
        user_context: Optional[UserContext] = None,
        conversation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Ask a question to Genie
        
        Args:
            question: Natural language question
            space_id: Genie space ID (uses default if not provided)
            user_context: User making the request
            conversation_id: ID for continuing a conversation
        
        Returns:
            Genie response with data, SQL, and visualizations
        """
        space = space_id or self.default_space_id
        if not space:
            raise ValueError("No Genie space ID provided")
        
        try:
            # Use user's workspace client for on-behalf-of execution
            client = user_context.workspace_client if user_context else self.client
            
            # Create or continue conversation
            if not conversation_id:
                conversation_id = str(uuid.uuid4())
            
            # Ask Genie (Note: Actual Genie SDK API may differ)
            # This is a simplified example - adjust based on actual Genie SDK
            response = await self._call_genie(
                client=client,
                space_id=space,
                question=question,
                conversation_id=conversation_id
            )
            
            return {
                "success": True,
                "conversation_id": conversation_id,
                "question": question,
                "response": response.get("text", ""),
                "sql_executed": response.get("sql", ""),
                "data": response.get("data", []),
                "visualizations": response.get("suggested_viz", []),
                "timestamp": datetime.utcnow().isoformat(),
                "user_email": user_context.email if user_context else "unknown"
            }
            
        except Exception as e:
            logger.error(f"Genie query failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "question": question,
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def _call_genie(
        self,
        client: WorkspaceClient,
        space_id: str,
        question: str,
        conversation_id: str
    ) -> Dict[str, Any]:
        """
        Call Genie API
        
        Note: This is a placeholder implementation.
        The actual Genie SDK API may differ.
        """
        # Placeholder for Genie API call
        # In production, use actual Genie SDK methods
        
        # Example structure (adjust based on actual API):
        # genie_client = client.genie
        # result = genie_client.spaces.ask(
        #     space_id=space_id,
        #     message=question,
        #     conversation_id=conversation_id
        # )
        
        # For now, return mock structure
        return {
            "text": f"I would analyze: {question}",
            "sql": "SELECT * FROM table WHERE condition = true",
            "data": [],
            "suggested_viz": ["bar", "line"]
        }
    
    async def get_conversation_history(
        self,
        conversation_id: str,
        user_context: Optional[UserContext] = None
    ) -> List[Dict[str, Any]]:
        """Get conversation history"""
        try:
            # Retrieve from database or Genie API
            # For now, return empty list
            return []
        except Exception as e:
            logger.error(f"Failed to get conversation history: {str(e)}")
            return []
    
    async def list_spaces(
        self,
        user_context: Optional[UserContext] = None
    ) -> List[Dict[str, Any]]:
        """List available Genie spaces for user"""
        try:
            client = user_context.workspace_client if user_context else self.client
            
            # List spaces (adjust based on actual Genie SDK)
            # spaces = client.genie.spaces.list()
            
            # Mock response
            return [
                {
                    "id": "space-1",
                    "name": "Health Insurance Analytics",
                    "description": "Ask questions about membership, claims, and utilization"
                }
            ]
        except Exception as e:
            logger.error(f"Failed to list Genie spaces: {str(e)}")
            return []
    
    async def pin_to_dashboard(
        self,
        conversation_id: str,
        message_id: str,
        user_context: Optional[UserContext] = None
    ) -> bool:
        """Pin a Genie result to the dashboard"""
        try:
            # Save to user preferences or dashboard config
            # Implementation depends on how you want to persist pinned items
            logger.info(f"Pinned message {message_id} from conversation {conversation_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to pin message: {str(e)}")
            return False


# Global Genie service instance
_genie_service: Optional[GenieService] = None


def get_genie_service(
    workspace_client: WorkspaceClient,
    default_space_id: Optional[str] = None
) -> GenieService:
    """Get or create global Genie service instance"""
    global _genie_service
    if _genie_service is None:
        _genie_service = GenieService(workspace_client, default_space_id)
    return _genie_service
