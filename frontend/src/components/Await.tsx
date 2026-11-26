import type { ReactNode } from "react";
import { type DataFile, useData } from "../lib/data";
import { ErrorState, Loading } from "./Evidence";

/** Render children once a data file is ready; standard loading and error states otherwise. */
export function Await<T>({ file, height, children }: { file: DataFile; height?: number; children: (data: T) => ReactNode }) {
  const state = useData<T>(file);
  if (state.status === "loading") return <Loading height={height} />;
  if (state.status === "error") return <ErrorState message={state.message} retry={() => window.location.reload()} />;
  return <>{children(state.data)}</>;
}

export function Await2<A, B>({ files, height, children }: { files: [DataFile, DataFile]; height?: number; children: (a: A, b: B) => ReactNode }) {
  const a = useData<A>(files[0]);
  const b = useData<B>(files[1]);
  if (a.status === "error") return <ErrorState message={a.message} retry={() => window.location.reload()} />;
  if (b.status === "error") return <ErrorState message={b.message} retry={() => window.location.reload()} />;
  if (a.status === "loading" || b.status === "loading") return <Loading height={height} />;
  return <>{children(a.data, b.data)}</>;
}
