let plugins = [];

if (process.env.NODE_ENV !== 'test') {
    try {
        // Dynamically import so missing optional deps don't crash tests/environments
        // that intentionally don't install Tailwind/PostCSS plugins.
        const tailwindMod = await import('tailwindcss').catch(() => null);
        const autoprefixerMod = await import('autoprefixer').catch(() => null);
        if (tailwindMod && autoprefixerMod) {
            const tailwind = tailwindMod?.default || tailwindMod;
            const autoprefixer = autoprefixerMod?.default || autoprefixerMod;
            plugins = [tailwind(), autoprefixer()];
        } else {
            // Leave plugins empty if imports fail
            plugins = [];
        }
    } catch (e) {
        // Non-fatal: continue with empty plugins if anything goes wrong
        // eslint-disable-next-line no-console
        console.warn('PostCSS: optional plugins not loaded, skipping Tailwind/Autoprefixer');
        plugins = [];
    }
}

export default { plugins };
