import { useState } from 'react'

export default function SearchBar({
  value,
  onSearch,
}: {
  value: string
  onSearch: (q: string) => void
}) {
  const [text, setText] = useState(value)

  return (
    <form
      className="search-bar"
      onSubmit={(e) => {
        e.preventDefault()
        onSearch(text)
      }}
    >
      <input
        type="text"
        placeholder="Search titles, content, and entities (any language)…"
        value={text}
        onChange={(e) => setText(e.target.value)}
      />
      <button type="submit">Search</button>
    </form>
  )
}
