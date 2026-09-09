# -*- coding: utf-8 -*-
"""
Конфіг 16 тематичних підбірок («настроїв») для генератора mood.

Набір покриває всю аудиторію — від 12-річної дівчинки до брутального бороданя — і теми
свідомо НЕ перетинаються за настроєм (кожна каже, що належить сусідній темі):
  laugh Сміх · no-think Пригоди · family Всією сім'єю · worlds Інші світи · adrenaline Адреналін ·
  detective Розслідування · thrill-nerves Страшно · dark Нуар · epic Епос · blow-mind Головоломка ·
  date-night Романтика · cry До сліз · cozy-rain Затишок · mood-up Натхнення · nostalgia Ретро ·
  background Фоном
Slug-и перейменованих тем (no-think, thrill-nerves, date-night, mood-up) лишено старими —
це імена файлів mood/{slug}.{uk,ru}.json, на які посилається клієнт.

Кожна тема містить:
  slug            — стабільний англ. ідентифікатор (ім'я файлів mood/{slug}.{uk,ru}.json)
  icon            — SVG-іконка настрою (fill/stroke=currentColor)
  title_uk/ru     — назва теми
  prompt          — опис теми для ІІ (англійською), з явними EXCLUDE
  styles          — курований список саб-жанрів; генератор ротує PRIMARY (за днем) × SECONDARY (за прогоном)
  craft           — нюанси якості/тону для ІІ
  tone_strict     — (опц.) жорсткий тон-гейт: лише безпечні run-модифікатори
  quality_min     — (опц.) власний поріг рейтингу (дефолт QUALITY_MIN у генераторі)
  judge_min       — (опц.) власний поріг ІІ-судді 0-10 (дефолт JUDGE_MIN у генераторі)
  judge_gate      — (опц.) власний HARD GATE для судді; дефолт для tone_strict: сумний/гіркий фінал або
                    важкий тон -> 0-3. Політика заборонена в усіх темах через TMDB-теги (генератор)
  min_year/max_year — (опц.) межі року релізу (дефолт MIN_YEAR у генераторі; «Ретро» має max_year)
  require_genre   — м'який жанровий гард фінальної вибірки (не-жанрові лише на добір)
  genre_whitelist / discover_movie / discover_tv — для /discover-пулу (вимкнено, DISCOVER_PAGES=0)

Довідник TMDB genre_id:
  movie: 28 Action, 12 Adventure, 16 Animation, 35 Comedy, 80 Crime, 99 Documentary,
         18 Drama, 10751 Family, 14 Fantasy, 36 History, 27 Horror, 10402 Music,
         9648 Mystery, 10749 Romance, 878 Sci-Fi, 53 Thriller, 10752 War, 37 Western
  tv:    10759 Action&Adventure, 16 Animation, 35 Comedy, 80 Crime, 99 Documentary,
         18 Drama, 10751 Family, 10762 Kids, 9648 Mystery, 10765 Sci-Fi&Fantasy,
         10766 Soap, 10768 War&Politics, 37 Western
"""

THEMES = [
    {
        'slug': 'laugh',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M256,0C114.833,0,0,114.833,0,256s114.833,256,256,256s256-114.833,256-256S397.167,0,256,0z M256,472.341c-119.275,0-216.341-97.066-216.341-216.341S136.725,39.659,256,39.659c119.295,0,216.341,97.066,216.341,216.341S375.275,472.341,256,472.341z"/><circle cx="176" cy="200" r="28" fill="currentColor"/><circle cx="336" cy="200" r="28" fill="currentColor"/><path fill="none" stroke="currentColor" stroke-width="28" stroke-linecap="round" d="M160 300a112 112 0 0 0 192 0"/></svg>',
        'title_uk': 'Сміх',
        'title_ru': 'Смех',
        'prompt': (
            'Broadly funny, laugh-out-loud crowd-pleasers people quote and rewatch for the laughs — '
            'mainstream and cult comedies, spoofs, slapstick, feel-good farces. EXCLUDE dry arthouse '
            'comedy, ANY political comedy or political satire, bleak satire, comedy-dramas that '
            "aren't actually funny (no Fleabag / The Lobster / The Death of Stalin / In the Loop / "
            'Wag the Dog / Dr. Strangelove), and anything that is fundamentally a SPORTS movie or a '
            'BIOGRAPHY / biopic.'
        ),
        'styles': [
            'buddy & mismatched-duo comedies',
            'raunchy gross-out comedies',
            'spoof & parody (Airplane! / Naked Gun lineage)',
            'slapstick & physical comedy',
            'workplace & ensemble comedies',
            'stoner & slacker comedies',
            'teen & high-school comedies — laugh-out-loud only (NOT coming-of-age dramas)',
            'action-comedies',
            'comedy crime capers',
            'fish-out-of-water comedies',
            'holiday & party comedies',
            'family-friendly comedies',
            'rom-coms that are genuinely funny',
            'mockumentary & improv comedies',
            'musical & song comedies (funny first)',
            'goofy horror-comedies (laugh-first)',
            'buddy road-trip comedies (laugh-out-loud, not soul-searching)',
            'wedding & party-disaster comedies',
            'British & Aussie broad comedies',
        ],
        'craft': (
            "Every pick must be a film you'd call a COMEDY first — its main job is to make you laugh. "
            'TEST: if a reasonable viewer would describe it as a drama (even a funny, touching or '
            'bittersweet one), EXCLUDE it. INCLUDE laugh-first films even if a little heartfelt (Mean '
            'Girls, Superbad, 10 Things I Hate About You, Booksmart); EXCLUDE drama-first '
            'coming-of-age / bittersweet indies (no Perks of Being a Wallflower, Moonlight, Lady '
            'Bird, Eighth Grade, Boyhood, Mean Creek, The Way Way Back, Little Miss Sunshine). Also '
            'not dry arthouse or bleak satire, and NEVER political — no political comedies or '
            'political satire (no The Death of Stalin, In the Loop, Wag the Dog, Dr. Strangelove, '
            'Veep). And NEVER a SPORTS movie or a BIOGRAPHY / biopic — even with funny moments those '
            'are not comedies (no sports dramas, no athlete/musician life-stories). Movies only, no '
            'TV series.'
        ),
        'require_genre': {35},
        'genre_whitelist': {35},
        'discover_movie': [35],
        'discover_tv': [],
    },
    {
        'slug': 'no-think',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><circle cx="256" cy="256" r="196" fill="none" stroke="currentColor" stroke-width="32"/><path fill="currentColor" d="M256 92l52 132-52 32-52-32z"/><path fill="none" stroke="currentColor" stroke-width="30" stroke-linejoin="round" d="M256 420l-52-132 52-32 52 32z"/><circle cx="256" cy="256" r="18" fill="currentColor"/></svg>',
        'title_uk': 'Пригоди',
        'title_ru': 'Приключения',
        'prompt': (
            'Adventure and wonder: journeys, quests, treasure hunts, jungles, pirates, lost worlds. '
            'Fun, escapist, big-hearted, enjoyed by kids and grown-ups alike. EXCLUDE grim, gory or '
            'cynical films; EXCLUDE pure comedies with no journey in them.'
        ),
        'styles': [
            'treasure-hunt & lost-city adventures (Indiana Jones lineage)',
            'pirate & seafaring adventures',
            'jungle & island adventures (fun, not grim)',
            'family expedition & road-trip adventures',
            'mythic quests & sword-and-sorcery adventures',
            'globe-trotting spy-adventure romps',
            'dinosaur, monster & creature adventures (thrilling, not scary)',
            'animated adventure epics (non-anime)',
            'kids-on-an-adventure classics (Goonies lineage)',
            'wilderness & mountain expeditions',
            'underwater & deep-sea adventures',
            'desert & ancient-ruins adventures',
            'time-travel & portal adventures',
            'video-game & board-game adaptations (Jumanji lineage)',
            'swashbuckling & musketeer adventures',
            'animal-companion journeys',
            'adventure comedies with real stakes',
            'space adventure romps (fun, not cerebral)',
            'heist & caper adventures',
            'Western adventures & frontier journeys',
        ],
        'craft': (
            'Escapist wonder with momentum and heart; the journey is the point. Watchable by a '
            '12-year-old and genuinely enjoyed by adults: nothing grim, gory or cynical. Movies over '
            'TV.'
        ),
        'quality_min': 6.3,
        'require_genre': {12, 14, 16, 28, 878, 10751, 10759},
        'genre_whitelist': {12, 14, 28, 10751, 10759},
        'discover_movie': [12],
        'discover_tv': [10759],
    },
    {
        'slug': 'family',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="none" stroke="currentColor" stroke-width="32" stroke-linejoin="round" d="M72 236L256 84l184 152v212H72z"/><path fill="currentColor" d="M256 396l-70-70c-22-22-22-58 0-80s58-22 70 10c12-32 48-32 70-10s22 58 0 80z"/></svg>',
        'title_uk': "Всією сім'єю",
        'title_ru': 'Всей семьёй',
        'prompt': (
            'Family movie night: heartwarming films kids and parents genuinely enjoy together. '
            'Western animation (Pixar / Disney / DreamWorks / Aardman lineage), live-action family '
            'films, gentle fantasy. EXCLUDE anything scary, violent, cynical or adult-themed; EXCLUDE '
            'Japanese anime.'
        ),
        'styles': [
            'Pixar-style heartfelt animation',
            'Disney & DreamWorks animated favorites',
            'stop-motion & Aardman-style animation',
            'talking-animal live-action films',
            'kids-and-magic gentle fantasy',
            'family sports & underdog films',
            'live-action fairy tales & fables',
            'holiday & Christmas family classics',
            'family comedies with heart',
            'animal & pet family films',
            'kid-inventor, school & summer-camp adventures',
            'modern animated hits (2010s-2020s)',
            'family musicals & song-filled films',
            'growing-up stories (warm, not heavy)',
            'friendship & first-adventure stories',
            'superhero films for the whole family',
            'classic 80s-90s family films',
            'European family animation (non-anime)',
        ],
        'craft': (
            'Genuinely watchable by adults, not babysitting fodder: heart, humor and craft. Roughly '
            'PG tone; nothing frightening, sad-heavy or bleak; no dying-pet or dying-friend '
            'tearjerkers (no Hachi, Marley & Me, Bridge to Terabithia). Movies over TV.'
        ),
        'tone_strict': True,
        'quality_min': 6.3,
        'require_genre': {12, 14, 16, 35, 10751},
        'genre_whitelist': {16, 10751},
        'discover_movie': [10751],
        'discover_tv': [10751],
    },
    {
        'slug': 'worlds',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><circle cx="256" cy="256" r="118" fill="none" stroke="currentColor" stroke-width="32"/><path fill="none" stroke="currentColor" stroke-width="28" stroke-linecap="round" d="M104 318c-58 38-80 78-60 100 30 34 150-6 270-90s190-180 160-214c-20-22-66-12-124 24"/></svg>',
        'title_uk': 'Інші світи',
        'title_ru': 'Другие миры',
        'prompt': (
            'Sci-fi and fantasy worlds to get lost in: space, future cities, magic, dystopias, alien '
            'encounters. Immersive world-building first. EXCLUDE puzzle-box mind-benders (a separate '
            'theme) and horror-first films.'
        ),
        'styles': [
            'space opera & starships',
            'hard sci-fi & first contact',
            'dystopian futures',
            'cyberpunk & neon megacities',
            'high fantasy & magic kingdoms',
            'urban fantasy & hidden magical worlds',
            'alien invasion & encounter films',
            'post-apocalyptic wastelands',
            'time-travel adventures (fun, not cerebral)',
            'superhero universes (world-first, not generic)',
            'robots, androids & AI futures',
            'space survival & exploration',
            'dark fantasy & fairy-tale retellings',
            'steampunk & alternate-history worlds',
            'sci-fi from Europe & Asia (live-action, non-anime)',
            'mythology-based fantasy',
            'video-game & comic-world adaptations that work',
            'colony & generation-ship stories',
            'near-future speculative sci-fi',
        ],
        'craft': (
            'Immersion and world-building over everything; the viewer should feel transported. '
            'Mainstream pillars welcome alongside lesser-known gems. Mostly movies; at most a third '
            'TV series.'
        ),
        'quality_min': 6.5,
        'require_genre': {12, 14, 28, 878, 10765},
        'genre_whitelist': {14, 878, 10765},
        'discover_movie': [878, 14],
        'discover_tv': [10765],
    },
    {
        'slug': 'adrenaline',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><polygon fill="currentColor" points="300,32 116,296 236,296 212,480 396,216 276,216"/></svg>',
        'title_uk': 'Адреналін',
        'title_ru': 'Адреналин',
        'prompt': (
            'Relentless action and tension — chases, heists, survival, ticking-clock thrillers. '
            'EXCLUDE slow burns and talky dramas.'
        ),
        'styles': [
            'single-location siege & survival',
            'car-chase & vehicular action',
            'heists under pressure',
            'ticking-clock thrillers',
            'martial-arts & fight-driven action',
            'real-time / one-shot intensity',
            'special-ops & military missions',
            'natural-disaster survival',
            'hitman & assassin action',
            'prison-break & escape',
            'revenge rampages',
            'manhunt & chase thrillers',
            'spy & espionage action',
            'vehicular mayhem (bike, boat, train)',
            'wilderness pursuit & survival chase',
            'protect-the-target / bodyguard thrillers',
            'kidnap-rescue ticking-clock',
            'sieges on transport (plane, train, ship)',
        ],
        'craft': (
            'Pace and tension are king — momentum must never sag; prefer lean, propulsive genre films '
            'over bloated blockbusters.'
        ),
        'require_genre': {12, 27, 28, 53, 80, 878, 10759},
        'genre_whitelist': {12, 28, 53, 10759},
        'discover_movie': [28],
        'discover_tv': [10759],
    },
    {
        'slug': 'detective',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><circle cx="212" cy="212" r="132" fill="none" stroke="currentColor" stroke-width="40"/><path stroke="currentColor" stroke-width="60" stroke-linecap="round" d="M318 318l130 130"/></svg>',
        'title_uk': 'Розслідування',
        'title_ru': 'Расследование',
        'prompt': (
            'Detective and mystery: investigations, whodunits, clever sleuths, true-crime cases; the '
            'pleasure of following clues to the answer. EXCLUDE horror and action-first thrillers; '
            'EXCLUDE bleak nihilistic noir (a separate theme).'
        ),
        'styles': [
            'classic whodunits (Agatha Christie / Knives Out lineage)',
            'detective series with one case per season',
            'serial-killer investigations (procedural, not gory)',
            'cozy & witty detective series',
            'true-crime-based investigations',
            'journalist & newsroom investigations',
            'Nordic & European crime series',
            'private-eye & hardboiled detectives',
            'courtroom & legal mysteries',
            'small-town murder mysteries',
            'period detectives (Victorian to 1970s)',
            'heist-investigation & con-artist puzzles',
            'missing-person mysteries',
            'forensic & profiler procedurals',
            'amateur-sleuth & odd-couple detective duos',
            'spy & espionage investigations',
            'locked-room & closed-circle mysteries',
            'cold-case & decades-old-secret mysteries',
            'Asian detective cinema (Korean, Chinese, Japanese live-action)',
            'detective comedies (clever, laugh-friendly)',
        ],
        'craft': (
            'The investigation must be the spine: clues, deduction, reveal. Mix film and TV freely; '
            'at most 5 TV series per list. Smart and satisfying, not gory or nihilistic.'
        ),
        'quality_min': 6.5,
        'require_genre': {18, 53, 80, 9648},
        'genre_whitelist': {80, 9648},
        'discover_movie': [9648],
        'discover_tv': [9648, 80],
    },
    {
        'slug': 'thrill-nerves',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="none" stroke="currentColor" stroke-width="32" stroke-linejoin="round" d="M256 56c-92 0-164 74-164 166v226l46-38 46 38 36-38 36 38 36-38 36 38 46-38 46 38V222c0-92-72-166-164-166z"/><ellipse cx="208" cy="222" rx="26" ry="36" fill="currentColor"/><ellipse cx="304" cy="222" rx="26" ry="36" fill="currentColor"/><ellipse cx="256" cy="310" rx="22" ry="30" fill="currentColor"/></svg>',
        'title_uk': 'Страшно',
        'title_ru': 'Страшно',
        'prompt': (
            'Horror first: dread, scares, terror, from goosebumps to terrifying. Suspense thrillers '
            "only if genuinely frightening. EXCLUDE action movies with monsters that aren't actually "
            'scary; EXCLUDE slow character dramas and crime procedurals that merely feel tense.'
        ),
        'styles': [
            'slow-burn atmospheric horror',
            'supernatural hauntings',
            'psychological horror',
            'folk horror',
            'home-invasion / survival horror',
            'genuinely scary creature horror',
            'possession & occult horror',
            'found-footage / mockumentary horror',
            'body horror',
            'cult & ritual horror',
            'isolation horror (cabin, remote, snowbound)',
            'monster-in-the-dark dread',
            'acclaimed foreign horror (Korean, Spanish, etc.)',
            'slasher & masked-killer horror',
            'vampire & werewolf horror',
            'zombie & outbreak horror',
            'haunted-house & haunted-object horror',
            'witch & folk-occult horror',
            'techno, screen & AI horror',
        ],
        'craft': (
            'Genuinely scary or suspenseful — dread and tension, not action; prefer acclaimed and '
            'cult horror over direct-to-video schlock; strong foreign horror welcome.'
        ),
        'require_genre': {27, 53, 9648, 10765},
        'genre_whitelist': {27, 53, 9648, 10765},
        'discover_movie': [27],
        'discover_tv': [9648],
    },
    {
        'slug': 'dark',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><ellipse cx="256" cy="342" rx="216" ry="46" fill="currentColor"/><path fill="currentColor" d="M160 342C146 224 172 132 202 132c18 0 26 24 54 24s36-24 54-24c30 0 56 92 42 210z"/></svg>',
        'title_uk': 'Нуар',
        'title_ru': 'Нуар',
        'prompt': (
            'Bleak, morally grey, oppressive — noir, psychological, disturbing. EXCLUDE merely '
            "'serious' prestige dramas that aren't actually dark. TV ok if genuinely bleak."
        ),
        'styles': [
            'neo-noir crime',
            'psychological character studies',
            'bleak revenge dramas',
            'slow-burn disturbing thrillers',
            'morally-grey crime sagas',
            'nihilistic downbeat dramas',
            'rural / small-town noir',
            'corruption & conspiracy noir',
            'grim serial-killer procedurals',
            'addiction & self-destruction dramas',
            'cold Nordic / European noir',
            'oppressive dystopian bleakness',
            'disturbing character-driven horror',
            'true-crime-inspired grimness',
            'hitman & underworld character studies',
            'bleak prison & incarceration dramas',
            'morally-grey legal & political noir',
            'cold-war espionage paranoia',
            'domestic-thriller dread (marriages gone dark)',
            'gangland rise-and-fall tragedies',
        ],
        'craft': (
            "Genuinely bleak and oppressive in tone, not merely 'serious'; morally grey; acclaimed or "
            'cult, not exploitation schlock.'
        ),
        'require_genre': {18, 27, 53, 80, 9648, 10768},
        'genre_whitelist': {18, 53, 80, 9648},
        'discover_movie': [80],
        'discover_tv': [80],
    },
    {
        'slug': 'epic',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><polygon fill="currentColor" points="16,436 176,140 288,340 344,248 496,436"/></svg>',
        'title_uk': 'Епос',
        'title_ru': 'Эпос',
        'prompt': (
            'Grand-scale spectacle — sweeping sagas, vast worlds, war and history on a huge canvas, '
            'for the big screen. EXCLUDE small, intimate chamber pieces.'
        ),
        'styles': [
            'historical war epics',
            'sweeping period sagas',
            'fantasy world-building epics (LOTR / GoT scale)',
            'sci-fi space spectacle (Dune / Star Wars scale)',
            'ancient-world / sword-and-sandal epics',
            'epic wilderness & survival spectacle (huge scale)',
            'revolution & empire sagas',
            'seafaring & swashbuckling adventure epics',
            'sprawling crime sagas of epic scope',
            'grand mythic & fantasy quests (Conan / Willow scale)',
            'disaster-scale spectacle',
            'grand historical-figure biopics',
            'medieval siege & castle-war epics',
            'naval & age-of-sail battle epics',
            'samurai & Asian historical epics (live-action, non-anime)',
            'biblical & mythological spectacle',
            'frontier & western epics (huge scale)',
            'dynastic & royal-court sagas',
        ],
        'craft': (
            'Scale and spectacle must feel huge — vast worlds, sweeping scope, big-screen grandeur; '
            'skip intimate small-canvas films. Lean MAINSTREAM here: the big beloved epics belong, '
            'mixed with a few lesser-seen ones.'
        ),
        'require_genre': {12, 14, 18, 28, 36, 37, 878, 10752, 10759, 10765, 10768},
        'genre_whitelist': {12, 14, 28, 36, 878, 10752, 10759, 10765, 10768},
        'discover_movie': [36, 10752, 14],
        'discover_tv': [10768],
    },
    {
        'slug': 'blow-mind',
        'icon': '<svg viewBox="0 0 875 753" xmlns="http://www.w3.org/2000/svg"><g transform="scale(0.5,-0.5) translate(875,-875)"><path fill="none" stroke="currentColor" stroke-width="100" stroke-linecap="round" stroke-linejoin="round" d="M351.817,-129.806 C367.151,-88.2461,375,-44.2981,375,0 C375,207.107,207.107,375,0,375 C-207.107,375,-375,207.107,-375,0 C-375,-76.957,-351.323,-152.051,-307.182,-215.091 C-283.64,-248.712,-271.012,-288.762,-271.012,-329.806 C-271.012,-440.263,-360.555,-529.806,-471.012,-529.806 C-536.265,-529.806,-597.415,-497.974,-634.843,-444.522 C-726.067,-314.24,-775,-159.045,-775,0 C-775,428.021,-428.021,775,0,775 C428.021,775,775,428.021,775,0 C775,-91.5496,758.779,-182.375,727.089,-268.2655 C669.104,-425.425,519.332,-529.806,351.817,-529.806 C196.046,-529.806,54.4429,-439.376,-11.0774,-298.055 C-30.5166,-256.177,-86.6373,-160.72,-123.471,-124.015 C-156.457,-91.1748,-175,-46.5459,-175,0 C-175,96.6497,-96.6497,175,0,175 C96.6497,175,175,96.6497,175,0 C175,-20.6724,171.337,-41.1814,164.181,-60.5761 C156.003,-82.7417,151.817,-106.181,151.817,-129.806 C151.817,-240.263,241.36,-329.806,351.817,-329.806 C435.574,-329.806,510.46,-277.616,539.453,-199.036 C562.965,-135.311,575,-67.9238,575,0 C575,317.564,317.564,575,0,575 C-317.564,575,-575,317.564,-575,0 C-575,-118.001,-538.695,-233.146,-471.012,-329.806"/></g></svg>',
        'title_uk': 'Головоломка',
        'title_ru': 'Головоломка',
        'prompt': (
            'Puzzle-box: time loops, unreliable narrators, reality-bending, rewatch-worthy twists. '
            'EXCLUDE straightforward blockbusters and space operas even if sci-fi.'
        ),
        'styles': [
            'time-loop films',
            'unreliable-narrator mind-benders',
            'fractured / nonlinear timelines',
            'reality- or simulation-questioning stories',
            'memory & identity puzzles',
            'rug-pull twist thrillers',
            'cerebral multiverse / parallel-worlds (not superhero)',
            "ambiguous 'what really happened' films",
            'clever low-budget high-concept sci-fi',
            'paranoia & conspiracy puzzle-boxes',
            'recursion & nested-structure narratives',
            'philosophical sci-fi that messes with your head',
            'con-artist rug-pull capers',
            'puzzle-box TV series',
            'rug-pull heist structures',
            'closed-room whodunit puzzles',
            'AI & consciousness mind-benders',
            'doppelganger & double-identity puzzles',
            'time-travel paradox films',
            'Rashomon-style perspective-shifters',
        ],
        'craft': (
            'Favor films whose structure or twist is the whole point and rewards a rewatch; skip '
            'anything whose twist is now common knowledge; clever indie concepts welcome.'
        ),
        'require_genre': {14, 18, 27, 53, 878, 9648, 10765},
        'genre_whitelist': {53, 878, 9648, 10765},
        'discover_movie': [878, 9648],
        'discover_tv': [10765],
    },
    {
        'slug': 'date-night',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M256 464C120 360 48 288 48 200c0-60 48-104 104-104 40 0 76 24 104 64 28-40 64-64 104-64 56 0 104 44 104 104 0 88-72 160-208 264z"/></svg>',
        'title_uk': 'Романтика',
        'title_ru': 'Романтика',
        'prompt': (
            'Romantic films for two — chemistry, longing, charming rom-coms, swoony love stories. '
            'EXCLUDE bleak or tragic romances that would kill the mood.'
        ),
        'styles': [
            'sweeping historical / costume period romances',
            'romantic musicals',
            'charming foreign-language romances (live-action — French, Korean, Italian; NO anime)',
            'epic romantic dramas',
            'screwball & witty-banter romances',
            'holiday & seasonal romances',
            'second-chance & later-in-life romances',
            'opposites-attract rom-coms',
            'friends-to-lovers stories',
            'enemies-to-lovers romances',
            'literary-adaptation romances (Austen & co.)',
            'summer & vacation romances',
            'sweet teen & high-school romances (fun and warm, not tragic — no Romeo + Juliet / A Walk to Remember)',
            'royalty & fairy-tale romances (charming)',
            'workplace & rivals-to-lovers rom-coms',
            'dance & music romances (swoony)',
            'reunited-lovers & letters-across-time (warm ending)',
            'cross-cultural & travel romances',
            'witty modern rom-coms with real chemistry',
            'classic 80s/90s romances people rewatch',
        ],
        'craft': (
            'Charming, warm chemistry that plays for two; keep it swoony and light, never tragic (no '
            'Romeo + Juliet, A Walk to Remember, Blue Valentine); span eras and styles — do NOT '
            'default to recent indie rom-coms.'
        ),
        'require_genre': {18, 35, 10749},
        'genre_whitelist': {18, 35, 10749},
        'discover_movie': [10749],
        'discover_tv': [10766],
    },
    {
        'slug': 'cry',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M256 64C190 180 128 250 128 320a128 128 0 1 0 256 0C384 250 322 180 256 64z"/></svg>',
        'title_uk': 'До сліз',
        'title_ru': 'До слёз',
        'prompt': (
            'Tearjerkers about love, loss, family, sacrifice that genuinely make you cry. EXCLUDE '
            "crime sagas or war epics that are merely 'serious'. TV ok if truly moving."
        ),
        'styles': [
            'terminal-illness & grief dramas',
            'parent-and-child bonds',
            'bittersweet lost-love romances',
            'coming-of-age heartbreak',
            'war-torn family separation',
            'loyal-animal companion tearjerkers',
            'losing-a-friend stories',
            'immigrant & family-sacrifice dramas',
            'dementia & memory-loss dramas',
            'long-distance / letters love stories',
            'based-on-a-true-story tragedies',
            'quiet dramas about goodbyes',
            'foreign-language emotional gut-punches',
            'redemption & reconciliation dramas',
            'single-parent & adoption tearjerkers',
            'sibling-bond dramas',
            'disability & resilience dramas',
            'found-family & orphan stories',
            'old-age & end-of-life dramas',
            'reunions after long separation',
        ],
        'craft': (
            'Must genuinely earn the tears through character, not cheap manipulation or TV-movie sap; '
            'foreign-language gut-punches very welcome.'
        ),
        'require_genre': {18, 10749},
        'genre_whitelist': {18, 10749},
        'discover_movie': [18],
        'discover_tv': [18],
    },
    {
        'slug': 'cozy-rain',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><rect x="104" y="176" width="216" height="208" rx="28" fill="currentColor"/><path fill="none" stroke="currentColor" stroke-width="28" d="M320 224h28a52 52 0 0 1 0 104h-28"/><path fill="none" stroke="currentColor" stroke-width="20" stroke-linecap="round" d="M168 92c-18 22 18 40 0 60M248 92c-18 22 18 40 0 60"/></svg>',
        'title_uk': 'Затишок',
        'title_ru': 'Уют',
        'prompt': (
            'Gentle low-stakes comfort with a warm, soothing feel: calm films, slice-of-life, gentle '
            'animation. EXCLUDE tension, violence, loud spectacle; EXCLUDE triumph-and-tears uplift '
            'arcs (a separate theme) and romance-first films (a separate theme).'
        ),
        'styles': [
            'gentle live-action slice-of-life (real people, NOT anime/animation)',
            'food & cooking comfort films',
            'small-town charm stories',
            'gentle Western/European stop-motion & hand-drawn animation (NO anime)',
            'warm friendship stories',
            'bookshop / cafe / quaint-setting films',
            'gentle seasonal & holiday comfort',
            'cozy village & countryside life (gentle live-action, warm — no war, action or peril)',
            'wholesome family gentle films',
            'gentle pet & animal comfort (warm, never sad)',
            'bakery, craft & garden slice-of-life',
            'warm intergenerational friendships',
            'gentle armchair-travel & scenic comfort',
            'cozy Christmas-market & seasonal warmth',
            'gentle British comfort comedies (Paddington / Calendar Girls tone)',
        ],
        'craft': (
            'Warm, soft, low-stakes above all — nothing tense, loud, sad or heavy; no war, trauma or '
            'grief (no The Breadwinner, Persepolis, Grave of the Fireflies). Quiet slice-of-life and '
            'gentle non-anime animation are gold. NO Japanese anime (including Ghibli). Calm over '
            'triumph: no underdog wins, big speeches or emotional climaxes.'
        ),
        'tone_strict': True,
        'judge_gate': 'a sad ending or a heavy, tense or loud tone scores 0-3; a gentle bittersweet note in an otherwise warm, calm film is fine',
        'require_genre': {14, 16, 18, 35, 99, 10402, 10749, 10751},
        'genre_whitelist': {16, 35, 10402, 10751},
        'discover_movie': [16, 10751],
        'discover_tv': [10751],
    },
    {
        'slug': 'mood-up',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><circle cx="256" cy="256" r="104" fill="currentColor"/><path fill="none" stroke="currentColor" stroke-width="32" stroke-linecap="round" d="M256 40v48M256 424v48M40 256h48M424 256h48M107 107l34 34M371 371l34 34M405 107l-34 34M141 371l-34 34"/></svg>',
        'title_uk': 'Натхнення',
        'title_ru': 'Вдохновение',
        'prompt': (
            'Uplifting and inspiring: underdog triumphs, true stories of grit, comebacks, mentors, '
            'dreams achieved; you finish wanting to do something. A happy or triumphant ending is '
            'REQUIRED. EXCLUDE bittersweet or tragic endings; EXCLUDE calm cozy slice-of-life (a '
            'separate theme).'
        ),
        'styles': [
            'underdog sports triumphs',
            'inspirational true stories (triumphant, not tragic)',
            'comeback & second-chance stories',
            'mentor & student stories (triumphant, no abusive mentors)',
            'dream-chasers who make it',
            'community-comes-together stories',
            'inventors, pioneers & against-the-odds biopics',
            'uplifting music & performance films',
            'feel-good coming-of-age with a real win',
            'rousing team & competition films',
            'survival-against-the-odds true stories (they make it)',
            'inspiring teachers & coaches',
            'rags-to-riches & self-made stories',
            'uplifting animation (non-anime)',
            'small-business & workplace success stories',
            'joyful musicals (happy ending only)',
            'inspiring space, science & exploration stories',
            'found-family stories with a triumphant ending',
        ],
        'craft': (
            'OVERRIDING GATE (beats the sub-style): EVERY pick must FEEL uplifting and end happily. '
            'Before adding a title, judge its overall tone and ending — if it is sad, heavy, bleak, '
            'tragic or bittersweet, DROP it however acclaimed or well-fitting (NO Whiplash, The '
            'Wrestler, Black Swan, Requiem for a Dream, Dancer in the Dark, Mary and Max, The '
            'Breadwinner, Persepolis, Grave of the Fireflies, Manchester by the Sea, Blue Valentine, '
            'Into the Wild, Wild). Earned uplift, not saccharine.'
        ),
        'tone_strict': True,
        'genre_whitelist': {35, 10402, 10749, 10751},
        'discover_movie': [10402, 10749],
        'discover_tv': [35],
    },
    {
        'slug': 'nostalgia',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><rect x="56" y="120" width="400" height="272" rx="28" fill="none" stroke="currentColor" stroke-width="28"/><circle cx="192" cy="248" r="40" fill="currentColor"/><circle cx="320" cy="248" r="40" fill="currentColor"/><path stroke="currentColor" stroke-width="24" stroke-linecap="round" d="M168 340h176"/></svg>',
        'title_uk': 'Ретро',
        'title_ru': 'Ретро',
        'prompt': (
            'Beloved iconic classics (~1980s-2000s) people grew up with and rewatch. EXCLUDE recent '
            'films and obscure deep cuts — this mood is shared memory.'
        ),
        'styles': [
            '80s teen coming-of-age classics',
            '90s action blockbusters',
            'beloved 80s/90s fantasy adventures',
            'family classics people rewatch',
            '90s rom-com favorites',
            'iconic sci-fi of the era',
            'classic 80s/90s comedy hits',
            'beloved non-anime animated classics',
            '80s/90s adventure quests',
            'cult classics of the era',
            'sports classics',
            'summer-blockbuster nostalgia',
            '90s/2000s teen comedies',
            'iconic 80s/90s horror classics',
            'classic Disney & animated-era favorites (non-anime)',
            'beloved buddy-cop & action-duo classics',
            '90s/2000s rom-com staples',
            'classic creature-feature blockbusters',
        ],
        'craft': (
            'Shared-memory icons from ~1980s-2000s people rewatch; here the OBVIOUS beloved ones ARE '
            'the point — no obscure deep cuts, nothing after 2010.'
        ),
        'max_year': 2010,
        'require_genre': {12, 14, 18, 27, 28, 35, 53, 80, 878, 10749, 10751, 10759, 10765},
        'genre_whitelist': {12, 14, 18, 28, 35, 878, 10751},
        'discover_movie': [12, 14],
        'discover_tv': [10759],
    },
    {
        'slug': 'background',
        'icon': '<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><path fill="none" stroke="currentColor" stroke-width="32" d="M104 288v-24a152 152 0 0 1 304 0v24"/><rect x="80" y="280" width="72" height="128" rx="28" fill="currentColor"/><rect x="360" y="280" width="72" height="128" rx="28" fill="currentColor"/></svg>',
        'title_uk': 'Фоном',
        'title_ru': 'Фоном',
        'prompt': (
            'Easy, low-attention comfort to half-watch while doing chores — sitcoms, cozy '
            'procedurals, light comedies and feel-good movies alike (TV OR film). EXCLUDE dense '
            'serialized dramas and anything that demands full attention.'
        ),
        'styles': [
            'classic multi-cam sitcoms',
            'single-cam workplace sitcoms',
            'cozy crime & detective procedurals',
            'feel-good ensemble comedy series',
            'lighthearted mystery-of-the-week',
            'adult animated sitcoms (non-anime)',
            'Britcoms & comfort panel shows',
            'easy feel-good comfort movies',
            'light comedy movies you can half-watch',
            'cozy rom-com movies',
            'family sitcoms & light family movies',
            'comfort rewatch favorites (TV or film)',
            'light medical & legal comfort procedurals',
            'cooking & baking competition shows',
            'gentle cozy period comfort series',
            'half-watchable animated sitcoms (non-anime)',
            'light feel-good docu-comfort',
            'easy rewatch action-comedy movies',
        ],
        'craft': (
            'Half-watchable, low-plot comfort you can drop in and out of — sitcoms, procedurals and '
            'easy light movies alike; comfort and rewatchability over prestige. Everything must be '
            'genuinely LIGHT and low-attention: no dense serialized dramas, no heavy or demanding '
            'films.'
        ),
        'require_genre': {18, 35, 80, 9648, 10751, 10759, 10765, 10766},
        'genre_whitelist': {18, 35, 80, 10759},
        'discover_movie': [],
        'discover_tv': [35, 80],
    },
]
