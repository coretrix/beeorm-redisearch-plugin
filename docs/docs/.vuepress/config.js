const config = require('./config/index')

module.exports = {
    base: '/beeorm-redisearch-plugin/',
    title: 'beeorm-redisearch-plugin',
    description: 'Plugin for BeeORM which enables integration with Redisearch',
    head: [
        ['link', {rel: "shortcut icon", href: "logo-favicon.png"}],
        ['meta', {name: 'theme-color', content: '#D7A318'}],
        ['meta', {name: 'apple-mobile-web-app-capable', content: 'yes'}],
        ['meta', {name: 'apple-mobile-web-app-status-bar-style', content: 'black'}]
    ],
    themeConfig: {
        repo: 'https://github.com/coretrix/beeorm-redisearch-plugin',
        docsRepo: 'https://github.com/coretrix/beeorm-redisearch-plugin',
        logo: '/logo-favicon-90x90.png',
        editLinks: true,
        docsDir: 'docs/docs',
        editLinkText: '',
        lastUpdated: true,
        smoothScroll: true,
        algolia: config.Algolia,
        navbar: config.Navigation,
        sidebar: config.Sidebar,
    },
    plugins: [
        ['@vuepress/plugin-search', config.Search],
        ['@vuepress/plugin-back-to-top', true],
        ['@vuepress/plugin-medium-zoom', true],
        ['vuepress-plugin-sitemap', { hostname: 'https://coretrix.github.io/beeorm-redisearch-plugin' }],
        // ['@vuepress/google-analytics', { 'ga': ''}]
    ]
}
