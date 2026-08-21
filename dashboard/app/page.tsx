import { redirect } from "next/navigation";

import { getActiveSession } from "@/auth";
import { DiscordSignIn } from "@/components/auth/discord-sign-in";
import { Reveal } from "@/components/ui/reveal";

export default async function LandingPage() {
  // Anyone already signed in is here by accident (a bookmark, usually).
  if (await getActiveSession()) redirect("/dashboard");

  return (
    <main
      id="main"
      className="mx-auto flex min-h-dvh w-full max-w-md flex-col items-center justify-center px-6 py-16 text-center"
    >
      <Reveal>
        <h1 className="text-4xl font-semibold tracking-tight">Vibey</h1>

        <div className="mt-8">
          <DiscordSignIn />
        </div>

        <p className="mt-4 text-xs text-fg-subtle">
          Server administrators only. Access is checked against Discord on every visit.
        </p>
      </Reveal>
    </main>
  );
}
