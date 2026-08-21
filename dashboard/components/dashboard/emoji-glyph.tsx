import { cn } from "@/lib/utils";

/**
 * One emoji, custom or unicode.
 *
 * A custom emoji is an image on Discord's CDN keyed by its ID; a unicode emoji
 * is just a character the font already draws. The tracker stores the former with
 * an ID and the latter with a null ID and the glyph as its name, so the null is
 * the switch between the two renderings.
 *
 * `.png` is requested even for animated emoji: a still frame is the right call
 * in a dense list, and it sidesteps needing to know the animated flag here.
 */
export function EmojiGlyph({
  emojiId,
  name,
  size = 28,
  className,
}: {
  emojiId: string | null;
  name: string;
  size?: number;
  className?: string;
}) {
  if (emojiId) {
    return (
      // eslint-disable-next-line @next/next/no-img-element
      <img
        src={`https://cdn.discordapp.com/emojis/${emojiId}.png?size=48`}
        alt={`:${name}:`}
        width={size}
        height={size}
        className={cn("inline-block object-contain", className)}
        loading="lazy"
      />
    );
  }
  return (
    <span
      className={cn("inline-flex shrink-0 items-center justify-center overflow-hidden", className)}
      style={{ width: size, height: size, fontSize: size * 0.9, lineHeight: 1 }}
      role="img"
      aria-label={name}
    >
      {name}
    </span>
  );
}
