"use client";

import "./markdown-styles.css";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeKatex from "rehype-katex";
import remarkMath from "remark-math";
import {
  cloneElement,
  FC,
  isValidElement,
  memo,
  useState,
  type ReactNode,
} from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { SyntaxHighlighter } from "@/components/thread/syntax-highlighter";

import { TooltipIconButton } from "@/components/thread/tooltip-icon-button";
import { cn } from "@/lib/utils";

import "katex/dist/katex.min.css";

interface CodeHeaderProps {
  language?: string;
  code: string;
}

const useCopyToClipboard = ({
  copiedDuration = 3000,
}: {
  copiedDuration?: number;
} = {}) => {
  const [isCopied, setIsCopied] = useState<boolean>(false);

  const copyToClipboard = (value: string) => {
    if (!value) return;

    navigator.clipboard.writeText(value).then(() => {
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), copiedDuration);
    });
  };

  return { isCopied, copyToClipboard };
};

const CodeHeader: FC<CodeHeaderProps> = ({ language, code }) => {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const onCopy = () => {
    if (!code || isCopied) return;
    copyToClipboard(code);
  };

  return (
    <div className="flex items-center justify-between gap-4 rounded-t-lg bg-zinc-900 px-4 py-2 text-sm font-semibold text-white">
      <span className="lowercase [&>span]:text-xs">{language}</span>
      <TooltipIconButton
        tooltip="Copy"
        onClick={onCopy}
      >
        {!isCopied && <CopyIcon />}
        {isCopied && <CheckIcon />}
      </TooltipIconButton>
    </div>
  );
};

const technicalIdentifierPattern = /(\b[A-Za-z0-9_.-]+::(?:\/[A-Za-z0-9_./-]+|[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*)|(?<![\w/])\/[A-Za-z0-9_.-]+\/|(?<![A-Za-z0-9_])(?:[A-Z][a-z0-9]+|[A-Z]+(?=[A-Z][a-z])){2,}(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?![A-Za-z0-9_])(?!(?:\s*[=:])))/g;

function emphasizeTechnicalIdentifiers(children: ReactNode): ReactNode {
  if (typeof children === "string") {
    return children.split(technicalIdentifierPattern).map((part, index) =>
      index % 2 === 1 ? <strong key={`identifier-${index}`}>{part}</strong> : part,
    );
  }
  if (Array.isArray(children)) {
    return children.map((child) => emphasizeTechnicalIdentifiers(child));
  }
  if (isValidElement<{ children?: ReactNode }>(children)) {
    if (children.type === "code" || children.type === "strong") return children;
    return cloneElement(children, {
      children: emphasizeTechnicalIdentifiers(children.props.children),
    });
  }
  return children;
}

const defaultComponents: any = {
  h1: ({ className, ...props }: { className?: string }) => (
    <h1
      className={cn(
        "mb-8 scroll-m-20 text-4xl font-extrabold tracking-tight last:mb-0",
        className,
      )}
      {...props}
    />
  ),
  h2: ({ className, ...props }: { className?: string }) => (
    <h2
      className={cn(
        "mt-6 mb-3 scroll-m-20 text-xl font-semibold tracking-tight first:mt-0 last:mb-0",
        className,
      )}
      {...props}
    />
  ),
  h3: ({ className, ...props }: { className?: string }) => (
    <h3
      className={cn(
        "mt-6 mb-4 scroll-m-20 text-2xl font-semibold tracking-tight first:mt-0 last:mb-0",
        className,
      )}
      {...props}
    />
  ),
  h4: ({ className, ...props }: { className?: string }) => (
    <h4
      className={cn(
        "mt-6 mb-4 scroll-m-20 text-xl font-semibold tracking-tight first:mt-0 last:mb-0",
        className,
      )}
      {...props}
    />
  ),
  h5: ({ className, ...props }: { className?: string }) => (
    <h5
      className={cn(
        "my-4 text-lg font-semibold first:mt-0 last:mb-0",
        className,
      )}
      {...props}
    />
  ),
  h6: ({ className, ...props }: { className?: string }) => (
    <h6
      className={cn("my-4 font-semibold first:mt-0 last:mb-0", className)}
      {...props}
    />
  ),
  p: ({ className, children, ...props }: { className?: string; children?: ReactNode }) => (
    <p className={cn("mt-5 mb-5 leading-7 first:mt-0 last:mb-0", className)} {...props}>
      {emphasizeTechnicalIdentifiers(children)}
    </p>
  ),
  a: ({ className, href, ...props }: { className?: string; href?: string }) => {
    const opensNewTab = /^https?:\/\//i.test(href ?? "");

    return (
      <a
        href={href}
        target={opensNewTab ? "_blank" : undefined}
        rel={opensNewTab ? "noopener noreferrer" : undefined}
        className={cn(
          "text-primary font-medium underline underline-offset-4",
          className,
        )}
        {...props}
      />
    );
  },
  blockquote: ({ className, ...props }: { className?: string }) => (
    <blockquote
      className={cn("border-l-2 pl-6 italic", className)}
      {...props}
    />
  ),
  ul: ({ className, ...props }: { className?: string }) => (
    <ul
      className={cn("my-5 ml-6 list-disc [&>li]:mt-2", className)}
      {...props}
    />
  ),
  ol: ({ className, ...props }: { className?: string }) => (
    <ol
      className={cn("my-5 ml-6 list-decimal [&>li]:mt-2", className)}
      {...props}
    />
  ),
  li: ({ className, children, ...props }: { className?: string; children?: ReactNode }) => (
    <li className={cn("mt-2", className)} {...props}>
      {emphasizeTechnicalIdentifiers(children)}
    </li>
  ),
  hr: ({ className, ...props }: { className?: string }) => (
    <hr
      className={cn("my-5 border-b", className)}
      {...props}
    />
  ),
  table: ({ className, ...props }: { className?: string }) => (
    <div className="my-5 w-full min-w-0 max-w-full overflow-x-auto rounded-lg">
      <table
        className={cn(
          "w-full min-w-max border-separate border-spacing-0",
          className,
        )}
        {...props}
      />
    </div>
  ),
  th: ({ className, ...props }: { className?: string }) => (
    <th
      className={cn(
        "bg-muted px-4 py-2 text-left font-bold first:rounded-tl-lg last:rounded-tr-lg [&[align=center]]:text-center [&[align=right]]:text-right",
        className,
      )}
      {...props}
    />
  ),
  td: ({ className, ...props }: { className?: string }) => (
    <td
      className={cn(
        "border-b border-l px-4 py-2 text-left last:border-r [&[align=center]]:text-center [&[align=right]]:text-right",
        className,
      )}
      {...props}
    />
  ),
  tr: ({ className, ...props }: { className?: string }) => (
    <tr
      className={cn(
        "m-0 border-b p-0 first:border-t [&:last-child>td:first-child]:rounded-bl-lg [&:last-child>td:last-child]:rounded-br-lg",
        className,
      )}
      {...props}
    />
  ),
  sup: ({ className, ...props }: { className?: string }) => (
    <sup
      className={cn("[&>a]:text-xs [&>a]:no-underline", className)}
      {...props}
    />
  ),
  pre: ({ className, ...props }: { className?: string }) => (
    <pre
      className={cn(
        "max-w-4xl overflow-x-auto rounded-lg bg-black text-white",
        className,
      )}
      {...props}
    />
  ),
  code: ({
    className,
    children,
    ...props
  }: {
    className?: string;
    children: React.ReactNode;
  }) => {
    const match = /language-(\w+)/.exec(className || "");

    if (match) {
      const language = match[1];
      const code = String(children).replace(/\n$/, "");

      return (
        <>
          <CodeHeader
            language={language}
            code={code}
          />
          <SyntaxHighlighter
            language={language}
            className={className}
          >
            {code}
          </SyntaxHighlighter>
        </>
      );
    }

    return (
      <code
        className={cn("rounded font-semibold", className)}
        {...props}
      >
        {children}
      </code>
    );
  },
};

const MarkdownTextImpl: FC<{ children: string; compact?: boolean }> = ({
  children,
  compact = false,
}) => {
  const components = compact
    ? {
        ...defaultComponents,
        p: ({ className, children, ...props }: { className?: string; children?: ReactNode }) => (
          <p className={cn("my-0 leading-6", className)} {...props}>
            {emphasizeTechnicalIdentifiers(children)}
          </p>
        ),
        ul: ({ className, ...props }: { className?: string }) => (
          <ul className={cn("my-2 ml-5 list-disc [&>li]:mt-1", className)} {...props} />
        ),
        ol: ({ className, ...props }: { className?: string }) => (
          <ol className={cn("my-2 ml-5 list-decimal [&>li]:mt-1", className)} {...props} />
        ),
        li: ({ className, children, ...props }: { className?: string; children?: ReactNode }) => (
          <li className={cn("mt-1 leading-6", className)} {...props}>
            {emphasizeTechnicalIdentifiers(children)}
          </li>
        ),
      }
    : defaultComponents;

  return (
    <div className="markdown-content min-w-0 max-w-full">
      <ReactMarkdown
        remarkPlugins={[remarkGfm, remarkMath]}
        rehypePlugins={[rehypeKatex]}
        components={components}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
};

export const MarkdownText = memo(MarkdownTextImpl);
