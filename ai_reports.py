"""
AI Report Generation using Databricks Foundation Models
Generates narrative reports based on dashboard data
"""

from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from auth_middleware import UserContext
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate AI-powered reports using Databricks Foundation Models"""
    
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        model_endpoint: str = "databricks-dbrx-instruct"
    ):
        self.client = workspace_client
        self.model_endpoint = model_endpoint
    
    async def generate_report(
        self,
        report_type: str,
        dashboard_data: Dict[str, Any],
        user_prompt: Optional[str] = None,
        user_context: Optional[UserContext] = None
    ) -> Dict[str, Any]:
        """
        Generate an AI report
        
        Args:
            report_type: Type of report ('churn', 'performance', 'comparative', 'executive', 'custom')
            dashboard_data: Dashboard data to analyze
            user_prompt: Optional custom prompt from user
            user_context: User making the request
        
        Returns:
            Generated report with metadata
        """
        start_time = datetime.utcnow()
        
        try:
            # Build context from dashboard data
            context = self._build_context(dashboard_data)
            
            # Get template prompt based on report type
            if user_prompt:
                prompt = user_prompt
            else:
                prompt = self._get_report_template(report_type)
            
            # Combine context and prompt
            full_prompt = f"""You are an expert health insurance analytics analyst. Analyze the following dashboard data and provide insights.

Dashboard Data:
{context}

Task:
{prompt}

Please provide a comprehensive, well-structured report with:
1. Executive Summary
2. Key Findings
3. Detailed Analysis
4. Recommendations
5. Conclusion

Format the response in Markdown."""
            
            # Call Databricks Foundation Model
            response = self._call_model(full_prompt)
            
            # Calculate metrics
            generation_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return {
                "success": True,
                "report": response["content"],
                "report_type": report_type,
                "generation_time_ms": int(generation_time),
                "tokens_used": response.get("tokens_used", 0),
                "model": self.model_endpoint,
                "generated_at": datetime.utcnow().isoformat(),
                "user_email": user_context.email if user_context else "unknown"
            }
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "report_type": report_type,
                "generation_time_ms": int((datetime.utcnow() - start_time).total_seconds() * 1000)
            }
    
    def _build_context(self, dashboard_data: Dict[str, Any]) -> str:
        """Build context string from dashboard data"""
        context_parts = []
        
        for key, value in dashboard_data.items():
            if isinstance(value, list) and len(value) > 0:
                # Summarize list data
                context_parts.append(f"\n## {key.replace('_', ' ').title()}")
                context_parts.append(f"Total records: {len(value)}")
                
                # Add sample data
                if len(value) <= 5:
                    context_parts.append(json.dumps(value, indent=2))
                else:
                    context_parts.append("Sample data:")
                    context_parts.append(json.dumps(value[:5], indent=2))
            
            elif isinstance(value, dict):
                context_parts.append(f"\n## {key.replace('_', ' ').title()}")
                context_parts.append(json.dumps(value, indent=2))
            
            else:
                context_parts.append(f"{key}: {value}")
        
        return "\n".join(context_parts)
    
    def _get_report_template(self, report_type: str) -> str:
        """Get report template based on type"""
        templates = {
            "churn": """Analyze member churn trends and identify:
1. Which member segments have the highest churn rates?
2. What are the key drivers of churn?
3. Which regions or product lines are most affected?
4. What recommendations would reduce churn?""",
            
            "performance": """Provide a comprehensive performance analysis:
1. Overall membership growth trends
2. Product line performance comparison
3. Regional performance insights
4. Key metrics that stand out (positive or concerning)
5. Recommendations for improvement""",
            
            "comparative": """Compare performance across dimensions:
1. Year-over-year trends
2. Regional comparisons
3. Product line comparisons
4. Identify best and worst performing segments
5. What explains the differences?""",
            
            "executive": """Create an executive summary for leadership:
1. Top 3 key insights (brief, impactful)
2. Critical metrics and their trends
3. Major risks or opportunities
4. Top 3 recommended actions
Keep it concise and actionable.""",
            
            "custom": """Analyze the dashboard data and provide comprehensive insights."""
        }
        
        return templates.get(report_type, templates["custom"])
    
    def _call_model(self, prompt: str) -> Dict[str, Any]:
        """
        Call Databricks Foundation Model endpoint
        
        Args:
            prompt: The prompt to send to the model
        
        Returns:
            Model response with content and metadata
        """
        try:
            # Use Databricks SDK to call Foundation Model
            response = self.client.serving_endpoints.query(
                name=self.model_endpoint,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.USER,
                        content=prompt
                    )
                ],
                temperature=0.7,
                max_tokens=2000
            )
            
            # Extract response content
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content
                
                # Count tokens (approximate if not provided)
                tokens_used = response.usage.total_tokens if response.usage else len(prompt.split()) + len(content.split())
                
                return {
                    "content": content,
                    "tokens_used": tokens_used
                }
            else:
                raise Exception("No response from model")
                
        except Exception as e:
            logger.error(f"Model call failed: {str(e)}")
            # Fallback to a simple response if model fails
            return {
                "content": f"Error generating report: {str(e)}",
                "tokens_used": 0
            }
    
    def generate_report_summary(self, full_report: str, max_length: int = 500) -> str:
        """Generate a brief summary of a longer report"""
        # Simple extraction of first paragraph or executive summary
        lines = full_report.split('\n')
        summary = []
        char_count = 0
        
        for line in lines:
            if 'executive summary' in line.lower():
                # Start capturing from executive summary
                summary = [line]
                continue
            
            if summary or char_count < max_length:
                summary.append(line)
                char_count += len(line)
                
                if char_count >= max_length:
                    break
        
        return '\n'.join(summary[:5])  # First 5 lines max


# Global report generator instance
_report_generator: Optional[ReportGenerator] = None


def get_report_generator(
    workspace_client: WorkspaceClient,
    model_endpoint: str = "databricks-dbrx-instruct"
) -> ReportGenerator:
    """Get or create global report generator instance"""
    global _report_generator
    if _report_generator is None:
        _report_generator = ReportGenerator(workspace_client, model_endpoint)
    return _report_generator
