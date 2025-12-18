import Link from 'next/link'
import React from 'react'

export default function HomePage() {
  return (
    <main className="min-h-screen bg-gradient-to-b from-gray-900 to-gray-800 text-white flex items-center">
      <div className="container mx-auto px-6 py-20">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-4xl sm:text-5xl md:text-6xl font-extrabold leading-tight">
            Trade Professional Athletes Like Stocks
          </h1>
          <p className="mt-6 text-gray-400 text-lg sm:text-xl">
            Bring performance-based value to athlete markets — track per-game performance, watch prices move with results, and manage your portfolio with confidence.
          </p>

          <div className="mt-10 flex justify-center gap-4">
            <Link href="/players" className="inline-block bg-emerald-500 hover:bg-emerald-400 text-white font-semibold py-3 px-6 rounded-lg shadow-lg transform transition-all duration-200 hover:-translate-y-0.5">
              Sign In
            </Link>
            <a
              href="#features"
              className="inline-block border border-gray-700 text-gray-200 hover:border-gray-500 hover:text-white py-3 px-5 rounded-lg"
            >
              Learn More
            </a>
          </div>
        </div>

        <section id="features" className="mt-16">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M3 3v18h18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M21 3l-6 6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Real-Time Trading</h3>
                  <p className="mt-1 text-gray-300 text-sm">Buy and sell athlete positions as performance updates arrive.</p>
                </div>
              </div>
            </article>

            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M12 3v18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M3 12h18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Performance Based</h3>
                  <p className="mt-1 text-gray-300 text-sm">Prices reflect real game performance — value moves with results.</p>
                </div>
              </div>
            </article>

            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M4 12h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M4 6h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M4 18h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Track Your Portfolio</h3>
                  <p className="mt-1 text-gray-300 text-sm">Monitor holdings, historical performance, and price trends in one place.</p>
                </div>
              </div>
            </article>
          </div>
        </section>
      </div>
    </main>
  )
}

export const dynamic = 'force-dynamic'
