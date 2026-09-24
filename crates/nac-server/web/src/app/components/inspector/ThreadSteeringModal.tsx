import { useState } from "react";

import { SteeringPromptModal } from "@/app/components/inspector/SteeringPromptModal";
import { toRunError } from "@/app/lib/providerError";
import { errorMessage, useToast } from "@/app/providers/ToastProvider";
import { useSteerThread } from "@/app/services/queries";

export function ThreadSteeringModal({
  sessionId,
  threadName,
  onClose,
}: {
  sessionId: string;
  threadName: string;
  onClose: () => void;
}) {
  const [instruction, setInstruction] = useState("");
  const steerThread = useSteerThread();
  const toast = useToast();

  const submit = async () => {
    const prompt = instruction.trim();
    if (!prompt) {
      toast.error("A steering message is required.");
      return;
    }
    try {
      await steerThread.mutateAsync({ id: sessionId, threadName, instruction: prompt });
      setInstruction("");
      onClose();
    } catch (error) {
      toast.error(`Unable to steer ${threadName}: ${errorMessage(toRunError(error))}`);
    }
  };

  return (
    <SteeringPromptModal
      open
      title={`Steer ${threadName}`}
      value={instruction}
      submitting={steerThread.isPending}
      onChange={setInstruction}
      onClose={onClose}
      onSubmit={() => void submit()}
    />
  );
}
