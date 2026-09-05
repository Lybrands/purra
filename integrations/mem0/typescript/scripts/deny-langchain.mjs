/** Test-only import tripwire, also used against installed artifacts. */
export async function resolve(specifier, context, nextResolve) {
  if (/^(?:@langchain\/|langchain(?:\/|$)|langsmith(?:\/|$))/.test(specifier)) {
    throw new Error(`LangChain import attempted: ${specifier}`);
  }
  return nextResolve(specifier, context);
}
