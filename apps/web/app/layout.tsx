import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NorthBound - FX Research Signals",
  description: "FX direction signals for CAD pairs delivered to your inbox. Research, not trading advice.",
  icons: {
    icon: "/brand/northbound-logo.png",
    shortcut: "/brand/northbound-logo.png",
    apple: "/brand/northbound-logo.png",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
