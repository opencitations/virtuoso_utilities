// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://opencitations.github.io',
	base: '/virtuoso_utilities',
	integrations: [
		starlight({
			title: 'Virtuoso Utilities',
			description: 'A collection of Python utilities for interacting with OpenLink Virtuoso',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/opencitations/virtuoso_utilities' },
			],
			sidebar: [
				{
					label: 'Getting started',
					items: [
						{ label: 'Introduction', slug: 'index' },
						{ label: 'Installation', slug: 'installation' },
					],
				},
				{
					label: 'Utilities',
					items: [
						{ label: 'Docker launcher', slug: 'utilities/launch-virtuoso' },
						{ label: 'Native entrypoint', slug: 'utilities/native-entrypoint' },
						{ label: 'Bulk loader', slug: 'utilities/bulk-load' },
						{ label: 'Quadstore dump', slug: 'utilities/dump-quadstore' },
						{ label: 'Full-text index rebuilder', slug: 'utilities/rebuild-index' },
					],
				},
				{
					label: 'Testing',
					items: [
						{ label: 'Benchmarks', slug: 'benchmarks' },
					],
				},
			],
			editLink: {
				baseUrl: 'https://github.com/opencitations/virtuoso_utilities/edit/master/docs/',
			},
		}),
	],
});
