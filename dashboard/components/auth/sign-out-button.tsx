"use client";

import { useTransition } from "react";
import { LogOut } from "lucide-react";

import { endSession } from "@/app/actions/auth";
import { Button } from "@/components/ui/button";

export function SignOutButton() {
  const [pending, startTransition] = useTransition();

  return (
    <Button
      variant="danger"
      loading={pending}
      onClick={() => startTransition(() => void endSession())}
    >
      <LogOut aria-hidden />
      Sign out
    </Button>
  );
}
