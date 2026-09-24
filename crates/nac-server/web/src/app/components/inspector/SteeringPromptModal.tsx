import type { ReactNode } from "react";

import { Button, ButtonVariant, Modal, ModalSize, TextArea, TextAreaSize } from "@/app/atoms";

export function SteeringPromptModal({
  open,
  title,
  subheader,
  value,
  submitting,
  disabled = false,
  footerLeading,
  onChange,
  onClose,
  onSubmit,
}: {
  open: boolean;
  title: string;
  subheader?: string;
  value: string;
  submitting: boolean;
  disabled?: boolean;
  footerLeading?: ReactNode;
  onChange: (value: string) => void;
  onClose: () => void;
  onSubmit: () => void;
}) {
  const close = submitting ? undefined : onClose;

  return (
    <Modal
      open={open}
      onClose={close}
      onNavigate={onClose}
      size={ModalSize.Wide}
      title={title}
      subheader={subheader}
    >
      <div className="flex flex-col gap-4">
        <TextArea
          label="Steering message"
          aria-label="Steering message"
          textAreaSize={TextAreaSize.Medium}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          isDisabled={submitting}
          textAreaClassName="h-[140px] resize-none"
        />
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>{footerLeading}</div>
          <Button
            variant={ButtonVariant.Primary}
            loading={submitting}
            disabled={disabled || submitting}
            onClick={onSubmit}
          >
            Send steering
          </Button>
        </div>
      </div>
    </Modal>
  );
}
