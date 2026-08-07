import Link from "next/link";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

export default function NotFound() {
  return (
    <main id="main" className="grid min-h-dvh place-items-center px-5 py-16">
      <Card className="w-full max-w-sm p-8 text-center">
        <p className="text-5xl font-semibold tracking-tight text-gradient">404</p>
        <h1 className="mt-4 text-lg font-semibold">That page doesn&apos;t exist</h1>
        <p className="mt-2 text-sm leading-relaxed text-fg-muted">
          The link may be out of date, or it points at a server this dashboard doesn&apos;t manage.
        </p>
        <Button asChild variant="secondary" className="mt-6 w-full">
          <Link href="/dashboard">Back to the dashboard</Link>
        </Button>
      </Card>
    </main>
  );
}
