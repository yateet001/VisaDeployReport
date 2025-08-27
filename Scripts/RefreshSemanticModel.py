from semantic_link import SemanticModel
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--workspaceId", required=True)
parser.add_argument("--modelId", required=True)
parser.add_argument("--accessToken", required=True)
args = parser.parse_args()

sm = SemanticModel(
    workspace_id=args.workspaceId,
    semantic_model_id=args.modelId,
    token=args.accessToken
)

print(f"🔄 Triggering refresh for model {args.modelId} in workspace {args.workspaceId}...")
sm.refresh()
sm.wait_for_refresh()
print("✅ Refresh completed successfully")
