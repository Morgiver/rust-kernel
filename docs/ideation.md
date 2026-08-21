# Kernel — vision complète

Ce document décrit le Kernel dans son intégralité : concepts, phases, surfaces
publiques, décisions techniques et frontières. Les choix laissés ouverts dans
l'ébauche initiale sont ici **tranchés**, pas énumérés. Le vocabulaire d'origine est
conservé (Kernel, Bundle, Component, Container, EventDispatcher/Listener,
Contracts) ; trois concepts sont ajoutés parce que rien dans la liste initiale ne
les portait : **Registry**, **Runnable**, **ExtensionPoint**.

Tout le code est en anglais. Les extraits sont des **signatures de référence**,
pas une implémentation.

---

## 0. Ce qu'est le Kernel — et ce qu'il n'est pas

Le Kernel est **une machine à états qui possède un graphe d'objets et un
ensemble de tâches longue durée, et qui garantit l'ordre dans lequel ils
naissent, tournent et meurent.**

Il **n'est pas** :

- un runtime : il ne crée pas le runtime async, il est une `Future` que
  l'application `await` ;
- un framework web, un ORM, un routeur, un parseur de ligne de commande : rien
  de tout cela n'est dans le Kernel, tout cela est un Bundle ;
- un conteneur de processus : un Kernel vit dans un processus, il n'en lance
  pas.

### Le Kernel ne possède pas le runtime

**Décision.** `Kernel::run()` est une `Future`. L'application choisit son
runtime, sa configuration de threads, et garde son `main`.

Conséquences acquises : les tests démarrent un Kernel dans un test async
standard ; un Kernel peut être embarqué dans une application qui a déjà son
propre `main` ; le Kernel n'a aucune opinion sur le nombre de threads.

**Abandonner la future ne saute pas l'arrêt.** Les phases 4 à 7 sont conduites
sur leur propre tâche ; `run()` ne fait que l'attendre. Abandonner cette future
*demande* un arrêt au lieu de le sauter, et la tâche le mène à terme. Ce que
l'appelant perd en abandonnant est l'`Outcome`, pas la libération des
ressources — un `select!` sur `run()` est donc sûr, tant que le runtime survit
à l'abandon.

Ce que le Kernel gère malgré tout : la capture des signaux d'arrêt (optionnelle,
désactivable), le compte à rebours d'arrêt, le code de sortie.

---

## 1. Vocabulaire — liste fermée

| Concept | Nature | Le Kernel en connaît-il l'instance ? |
|---|---|---|
| **Kernel** | La machine à états | — |
| **Bundle** | Unité de distribution et d'enregistrement | Oui, par sa liste ordonnée |
| **Registry** | Cahier d'enregistrement rempli en phase 2 | Oui |
| **Contract** | Un trait + son identité de résolution | Oui, par identité |
| **Container** | Résolution et durée de vie des instances | Oui |
| **Component** | Unité **avec** lifecycle géré (boot/shutdown) | Oui |
| **Service** | Unité **sans** lifecycle, résolue à la demande | Non, seulement son fournisseur |
| **Runnable** | Tâche longue durée supervisée | Oui |
| **ExtensionPoint** | Collection typée alimentée par plusieurs Bundles | Oui, par identité |
| **EventDispatcher / Listener** | Diffusion d'événements typés | Oui |

Ce qui **n'est pas** dans cette liste et n'y entrera pas : `Controller`,
`Command`, `Route`, `Migration`, `Job`, `Middleware`. Ce sont des concepts de
transport ou de domaine ; ils vivent dans les composants qui les définissent, et
le Kernel leur fournit le seul mécanisme dont ils ont besoin : `ExtensionPoint`
(§ 9). Voir la règle d'altitude, § 16.

---

## 2. Lifecycle — sept phases, ordre strict, aucune n'est sautée

```
Configure → Register → Resolve → Boot → Run → Shutdown → Terminated
```

| # | Phase | Nature | Qui agit | Échec = |
|---|---|---|---|---|
| 1 | **Configure** | sync | Le builder charge les sources de config, fusionne l'arbre | Le Kernel n'existe pas |
| 2 | **Register** | sync | Chaque Bundle remplit le `Registry` | Le Kernel n'existe pas |
| 3 | **Resolve** | sync | Le Kernel valide le graphe : contrats satisfaits, pas de cycle, ordre topologique | Le Kernel n'existe pas |
| 4 | **Boot** | async | Les Components sont instanciés et démarrés en ordre topologique | Rollback (§ 12) puis sortie |
| 5 | **Run** | async | Le superviseur lance les Runnables et les surveille | Selon criticité (§ 8) |
| 6 | **Shutdown** | async | Arrêt en deux temps, ordre inverse du boot réel | Journalisé, code de sortie |
| 7 | **Terminated** | — | Le Kernel rend un `Outcome` au `main` | — |

### La règle qui tient tout : rien ne se résout tard

> **Toute erreur de graphe (contrat manquant, cycle, ambiguïté, config invalide)
> doit apparaître au plus tard en phase 3.** Aucune résolution paresseuse n'est
> autorisée pendant `Run`.

C'est le prix — et la contrepartie — de la résolution dynamique (§ 5). Une
résolution qui échouerait à la première requête HTTP en production est un défaut
de conception, pas une erreur d'exploitation.

### Register et Resolve sont deux phases, pas une

L'ébauche initiale écrivait : « lors de l'enregistrement, le bundle enregistre tout ce
qu'il doit enregistrer **et charge tout ce dont il a besoin** ». Impossible :
quand le Bundle A s'enregistre, le Bundle B n'existe pas encore.

**Décision.** `Bundle::register` est **synchrone**, ne reçoit **pas** le
Container, et n'a **aucun** moyen d'atteindre un autre Bundle. Il ne fait que
déclarer. La consommation n'a lieu qu'en phase 4, via des fermetures que le
Container appellera lui-même.

---

## 3. Bundle

### Surface

```rust
pub trait Bundle: Send + Sync + 'static {
    fn manifest(&self) -> BundleManifest;
    fn register(&self, registry: &mut Registry) -> Result<(), RegisterError>;
}

pub struct BundleManifest {
    pub name: &'static str,
    pub version: &'static str,
    /// Contracts this bundle needs someone else to provide.
    pub requires: &'static [ContractRef],
    /// Bundles that must be registered before this one (rare; prefer contracts).
    pub after: &'static [&'static str],
}
```

`requires` est **redondant avec le graphe réel** et c'est voulu : il permet
d'émettre en phase 3 un diagnostic de haut niveau (« le bundle `billing` exige
le contrat `Clock`, fourni par aucun bundle enregistré ») avant la marche du
graphe, donc un message lisible plutôt qu'une erreur de résolution profonde. Un
`requires` mensonger est détecté en phase 3 et refusé.

### Forme physique

Un Bundle **implémenté** = une crate. Une feature **distribuée** = un workspace :

```
my-feature/                       # workspace de distribution
  crates/
    my-feature-contracts/         # traits publics uniquement — dépend de kernel-core
    my-feature-component/         # optionnel : le bloc technique réutilisable
    my-feature-bundle/            # la crate qui implémente Bundle
```

**Règle d'isolation, et c'est la principale du système :**

> Une crate `*-bundle` ne figure JAMAIS dans les dépendances d'une autre crate
> `*-bundle`. Un bundle ne dépend que de crates `*-contracts`.

Cette règle est **gardée en CI** (§ 16), pas confiée à la vigilance.

### Enregistrement

```rust
Kernel::builder()
    .bundle(FirstBundle::new())
    .bundle(SecondBundle::new())
```

L'ordre de déclaration est l'ordre d'enregistrement, il est déterministe, et il
sert d'unique départage des ex æquo dans le tri topologique. Il ne dicte **pas**
l'ordre de boot.

---

## 4. Contracts

**Un Contract est un trait `Send + Sync + 'static`, publié dans une crate
`*-contracts`, et utilisé comme clé de résolution dans le Container.**

C'est le **seul** moyen légal pour un Bundle d'atteindre une capacité fournie
par un autre Bundle. Ce n'est pas « si besoin » : c'est le mécanisme central de
découplage.

```rust
// in my-feature-contracts
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}
```

- **Identité** : `ContractId { type_id: TypeId::of::<dyn Clock>(), name: Option<&'static str> }`.
- **Fourniture** : `registry.provide::<dyn Clock>(provider)`.
- **Consommation** : `container.get::<dyn Clock>() -> Arc<dyn Clock>`.
- **Vérification** : phase 3, **toutes les violations agrégées**, jamais la
  première seule.

### Implémentations multiples

Autorisé, et nécessaire (points d'extension, variantes par environnement) :

```rust
registry.provide_named::<dyn Storage>("primary", p1).as_default();
registry.provide_named::<dyn Storage>("archive", p2);

container.get::<dyn Storage>()                    // le défaut
container.get_named::<dyn Storage>("archive")
container.get_all::<dyn Storage>()                // toutes, ordre d'enregistrement
```

Deux fournitures non nommées du même contrat = **erreur de phase 3**, pas un
écrasement silencieux.

### Où vit un contract

Dans une crate à part, dépendant de `kernel-core` seulement (§ 15). Un contract
ne dépend jamais du Kernel runtime, ce qui lui permet d'être stable et léger,
et ce qui garde les crates `*-contracts` compilables sans Tokio.

---

## 5. Container

### La décision structurante : résolution dynamique

**Décision. Le Container résout dynamiquement, par effacement de type, validé
en phase 3.**

Justification mesurable, pas esthétique :

- une résolution statique par génériques oblige l'application à nommer, dans ses
  types, la totalité du graphe de dépendances ; le Bundle A devrait alors nommer
  le type concret fourni par le Bundle B — ce qui détruit exactement la propriété
  recherchée ;
- le coût de la résolution dynamique est le report d'erreurs à l'exécution ; ce
  coût est **payé une fois, en phase 3**, avant que quoi que ce soit ne tourne.
  Il n'est jamais payé en phase 5.

### Durées de vie

| Durée de vie | Instanciation | Portée |
|---|---|---|
| `Shared` | une fois, en Boot | le processus |
| `Scoped` | une fois par `Scope` | l'unité de travail (une requête, un message) |
| `Factory` | à chaque `get` | l'appelant |

`Shared` est le défaut. `Scope` est un container enfant créé par celui qui
possède l'unité de travail (typiquement un Component de transport) ; il porte
sa propre table et délègue au parent pour tout ce qui est `Shared`.

### Fourniture

```rust
pub struct Provider<C: ?Sized + 'static> {
    pub lifetime: Lifetime,
    /// Declared dependencies — the graph the kernel checks in phase 3.
    pub requires: Vec<ContractRef>,
    pub build: BuildFn<C>,
}

pub type BuildFn<C> = Box<
    dyn for<'a> Fn(&'a Container) -> BoxFuture<'a, Result<Arc<C>, BuildError>>
        + Send + Sync + 'static,
>;
```

`requires` est déclaratif parce que Rust n'offre aucune introspection : le
Kernel ne peut pas deviner ce que `build` va résoudre. Le risque est un
`requires` désynchronisé de `build`.

**Garde vérifiable.** En build de debug, le Container instrumente les
résolutions faites pendant `build` et **échoue** si l'une n'était pas déclarée.
La frontière tient par une vérification, pas par une convention.

### Effacement de type — la mécanique exacte

C'est le point qui décide si l'architecture compile.

Un `Arc<dyn Any + Send + Sync>` construit depuis un `Arc<dyn Clock>` perdrait la
vtable du contrat. La valeur stockée est donc **l'objet-trait lui-même**,
lui-même effacé :

```rust
// store
let value: Arc<dyn Clock> = provider.build(&container).await?;
let erased: Arc<dyn Any + Send + Sync> = Arc::new(value);

// retrieve
let inner: Arc<Arc<dyn Clock>> = erased.downcast().map_err(|_| ResolveError::TypeMismatch)?;
let clock: Arc<dyn Clock> = Arc::clone(&*inner);
```

Une indirection `Arc` supplémentaire, payée une fois par résolution, jamais dans
un chemin chaud. Toute la partie fragile est confinée dans **un seul module** du
Container, avec une surface auditée.

### Règles invariantes

- Tout ce que le Container conserve est `Send + Sync + 'static`.
- Le Container ne rend **jamais** de `&mut`. La mutabilité intérieure est le
  problème du fournisseur.
- Après la phase 4, la table `Shared` est **immuable** : plus aucune écriture
  n'est possible, le conteneur la refuse. La lecture prend toujours un verrou
  partagé, non contendu par construction ; ce n'est pas un chemin chaud
  applicatif, puisqu'un appelant qui a résolu tient un `Arc` et appelle
  directement.
- Les cycles sont détectés en phase 3 par tri topologique et rapportés avec le
  cycle complet, pas avec un seul nœud.

---

## 6. Registry

Le cahier que les Bundles remplissent en phase 2. Surface unique et fermée — la
liste de ce qu'un Bundle peut enregistrer n'a **pas** de « etc. » :

```rust
impl Registry {
    // Contracts
    fn provide<C: ?Sized + 'static>(&mut self, provider: Provider<C>) -> Binding<'_, C>;
    fn provide_named<C: ?Sized + 'static>(&mut self, name: &'static str, provider: Provider<C>) -> Binding<'_, C>;

    // Lifecycle-managed units
    fn component<T: Component>(&mut self, provider: Provider<T>) -> Binding<'_, T>;

    // Long-running units
    fn runnable<T: Runnable>(&mut self, provider: Provider<T>) -> Binding<'_, T>;

    // Events
    fn listen<E: Event, L: Listener<E>>(&mut self, listener: L, priority: Priority);

    // Extension points
    fn declare_extension_point<X: Extension>(&mut self);
    fn contribute<X: Extension>(&mut self, item: X);
}
```

**Sept verbes d'enregistrement** — `provide`, `provide_named`, `component`,
`runnable`, `listen`, `declare_extension_point`, `contribute` — et rien d'autre
n'entre dans le Kernel par cette porte. Toute demande d'un huitième verbe est
une décision d'architecture, pas un ajout.

S'y ajoutent deux **accesseurs** en lecture seule, qui n'enregistrent rien :

```rust
impl Registry {
    /// Typed read of a configuration section.
    fn config<T: FromConfig>(&self, prefix: &str) -> Result<T, ConfigError>;
    fn telemetry(&self) -> &dyn Telemetry;
}
```

Ils donnent au Bundle ce dont il a besoin pour construire ses déclarations. Ils
prennent `&self`, ne touchent pas la table d'enregistrement, et ne font donc pas
partie de la surface fermée.

---

## 7. Component et Service — la distinction

L'ébauche initiale faisait cohabiter « components » et « services » sans les séparer.
La séparation retenue est **opérationnelle**, donc vérifiable :

> **Un Component a un lifecycle géré par le Kernel. Un Service n'en a pas.**

| | Component | Service |
|---|---|---|
| Reçoit `boot` / `shutdown` | Oui | Non |
| Possède une ressource (pool, socket, client) | Typiquement | Non |
| Instancié quand | Phase 4, ordre topologique | À la première résolution |
| Connu du Kernel par identité | Oui | Non |
| Peut déclarer un `ExtensionPoint` | Oui | Non |

Les deux « types » de Component de l'ébauche initiale — l'enveloppe d'une crate
existante et le bloc natif — **partagent exactement la même surface**. La
différence est ce qu'ils possèdent, pas ce qu'ils sont. Il n'y a donc **qu'un**
trait :

```rust
pub trait Component: Send + Sync + 'static {
    /// The one declared name, read at registration where `Self` is concrete.
    fn name() -> &'static str
    where
        Self: Sized;

    fn descriptor(&self) -> ComponentDescriptor;

    fn boot<'a>(&'a self, cx: &'a BootContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>>;

    fn shutdown<'a>(&'a self, cx: &'a ShutdownContext<'a>) -> BoxFuture<'a, Result<(), ComponentError>> {
        let _ = cx;
        Box::pin(async { Ok(()) })
    }
}

pub struct ComponentDescriptor {
    pub boot_timeout: Option<Duration>,
    pub shutdown_timeout: Option<Duration>,
}
```

`BootContext` donne accès : au Container (résolution autorisée, le graphe est
validé), à la config, au dispatcher, et à la collecte des `ExtensionPoint`.

Un Component **n'a pas le droit** de bloquer indéfiniment dans `boot` : le
`boot_timeout` est appliqué par le Kernel et son dépassement est une erreur de
phase 4.

Un Component qui doit faire tourner quelque chose en continu **n'utilise pas
`boot` pour ça** : il enregistre un `Runnable`. `boot` prépare, `run` tourne.

---

## 8. Runnable et supervision

C'est l'ajout le plus important à l'ébauche initiale : rien n'y portait « ce qui
tourne ». Un graphe d'objets sans tâche longue durée est un programme qui se
termine immédiatement.

```rust
pub trait Runnable: Send + Sync + 'static {
    /// The one declared name, read at registration where `Self` is concrete.
    fn name() -> &'static str
    where
        Self: Sized;

    fn descriptor(&self) -> RunnableDescriptor;
    fn run(self: Arc<Self>, cx: RunContext) -> BoxFuture<'static, Result<(), RunError>>;
}

pub struct RunnableDescriptor {
    pub criticality: Criticality,
    pub restart: RestartPolicy,
}

pub enum Criticality {
    /// Its termination stops the whole kernel.
    Essential,
    /// Its termination is recorded; the kernel keeps running.
    Ancillary,
}

pub enum RestartPolicy {
    Never,
    OnFailure { max_attempts: u32, backoff: Backoff },
}
```

### Contrat imposé au Runnable

1. `run` **doit** rendre la main quand le jeton d'arrêt de `RunContext` se
   déclenche. Un Runnable qui l'ignore est tué au terme du délai de grâce et
   c'est comptabilisé comme un arrêt sale.
2. Un `panic` dans `run` est intercepté à la jointure et traité comme un
   `RunError`, jamais propagé au Kernel.
3. Tous les Runnables démarrent **après** que tous les Components ont booté. Un
   Runnable ne dépend jamais d'un autre Runnable.

### Ce que fait le superviseur

- lance tous les Runnables, garde leurs poignées ;
- attend le premier des événements : un Runnable `Essential` se termine (quel
  que soit le résultat), un signal d'arrêt, un appel programmatique à
  `handle.shutdown()` ;
- applique la politique de redémarrage aux `Ancillary` en échec ;
- déclenche la phase 6.

---

## 9. ExtensionPoint — ce qui remplace « controllers » et « commands »

L'ébauche initiale listait `controllers/` et `commands/` comme des dossiers. Les
promouvoir en types du Kernel serait une faute d'altitude : `Controller`
présuppose un transport, `Command` présuppose une ligne de commande.

**Décision.** Le Kernel ne connaît ni l'un ni l'autre. Il fournit le seul
mécanisme générique dont ils ont besoin : **une collection typée qu'un Component
déclare et que n'importe quel Bundle alimente.**

```rust
pub trait Extension: Send + Sync + 'static {}

// A component declares the point it will consume (in ITS contracts crate)
registry.declare_extension_point::<HttpRoute>();

// Any bundle contributes to it
registry.contribute::<HttpRoute>(HttpRoute::new("/orders", handler));

// The owning component collects at boot
let routes: Vec<HttpRoute> = cx.collect::<HttpRoute>();
```

Règles :

- contribuer à un point **non déclaré** est une erreur de phase 3 ;
- un point déclaré et vide est valide ;
- l'ordre de collecte est l'ordre d'enregistrement des Bundles, donc
  déterministe et reproductible.

Le même mécanisme sert, sans aucun code supplémentaire dans le Kernel : routes
HTTP, commandes CLI, tâches planifiées, migrations, sondes de santé,
intercepteurs. `HttpRoute`, `CliCommand`, `Migration` sont définis par leurs
Components respectifs — **jamais** par le Kernel.

---

## 10. EventDispatcher / Listener

### Événements typés, pas nommés

**Décision.** Un événement est un type. La table de diffusion est indexée par
`TypeId`. Pas de chaînes de caractères : le type **est** le contrat, la
compilation vérifie la charge utile.

```rust
pub trait Event: Send + Sync + 'static {
    const NAME: &'static str; // diagnostics only
}

pub trait Listener<E: Event>: Send + Sync + 'static {
    fn on_event<'a>(
        &'a self,
        event: &'a mut E,
        cx: &'a ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<Flow, ListenerError>>;
}

pub enum Flow { Continue, Stop }
```

### Deux modes, explicitement distincts

```rust
impl EventDispatcher {
    /// Sequential, awaited, priority-ordered. Errors reach the emitter.
    /// Listeners may mutate the event and stop propagation.
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<Dispatched, DispatchError>;

    /// Detached. Ordering not guaranteed. Errors are reported to telemetry only.
    pub fn emit<E: Event>(&self, event: E);
}
```

`dispatch` sert quand le flot de contrôle de l'émetteur dépend du résultat
(enrichissement, veto, validation en chaîne). `emit` sert à la notification.
Confondre les deux est la principale source de bugs dans ce genre de mécanisme,
d'où deux méthodes et non un booléen.

### Enregistrement figé

Les Listeners ne s'enregistrent **qu'en phase 2**. Après la phase 3, la table
est immuable — donc lisible sans verrou pendant toute la phase 5. Aucun
enregistrement dynamique de Listener n'est prévu.

Ordre : `Priority` explicite (entier, décroissant), puis ordre d'enregistrement
des Bundles pour les ex æquo. Toujours déterministe.

### Événements de lifecycle du Kernel

Le Kernel émet ses propres événements ; c'est le levier de découplage principal,
et il est de première classe :

| Événement | Phase | Mode |
|---|---|---|
| `BundleRegistered` | 2 | `emit` |
| `GraphResolved` | 3 | `emit` |
| `BootStarted` / `ComponentBooted` / `BootCompleted` | 4 | `emit` |
| `Running` | 5 | `emit` |
| `ShutdownRequested` | 6 | `dispatch` (un Listener peut ajouter du contexte) |
| `Draining` / `Stopping` / `Stopped` | 6 | `emit` |

### Frontière

Le dispatcher est **en mémoire, intra-processus, sans persistance**. Faire
franchir une frontière de processus à un événement est le travail d'un Bundle
qui écoute et relaie. Le Kernel reste agnostique du transport.

---

## 11. Configuration

Absente de l'ébauche initiale, indispensable : elle conditionne la phase 2.

### Chaînage

```
defaults (code) → sources déclarées, dans l'ordre → surcharges explicites du builder
```

Le dernier gagne. La fusion est **par feuille**, pas par bloc : surcharger une
clé ne détruit pas ses voisines.

### Le Kernel n'impose aucun format

```rust
pub trait ConfigSource: Send + Sync + 'static {
    fn load(&self) -> Result<ConfigTree, ConfigError>;
}
```

`ConfigTree` est une structure de données du Kernel : arbre de `Map` / `Seq` /
`Scalar`. Le Kernel embarque **deux** sources seulement : variables
d'environnement et arbre en mémoire. Tout format de fichier est fourni par
l'application ou par un Bundle. Le Kernel ne dépend d'aucune bibliothèque de
sérialisation.

### Typage par Bundle

```rust
pub trait FromConfig: Sized {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError>;
}

// in Bundle::register
let cfg: HttpConfig = registry.config::<HttpConfig>("http")?;
```

La désérialisation a lieu **en phase 2** : une configuration invalide empêche le
Kernel d'exister, elle ne casse pas un Component à la phase 4.

Trade-off assumé : `FromConfig` est un trait maison, donc un dérive maison,
plutôt qu'une dépendance de sérialisation imposée à toutes les crates
`*-contracts`. Un pont vers une bibliothèque de sérialisation est possible
derrière un drapeau de compilation, à la charge de l'application.

### Secrets

```rust
pub struct Secret<T>(T);
```

`Debug` et `Display` sont rédigés. L'accès à la valeur passe par une méthode
nommée explicitement. Coût nul, supprime la fuite la plus fréquente.

### Rechargement à chaud

Non prévu. Voir § 19 — à confirmer.

---

## 12. Modèle d'erreur

### Une erreur par phase, jamais générique

```rust
pub enum KernelError {
    Config(Vec<ConfigError>),
    Register(Vec<RegisterError>),
    Resolve(Vec<ResolveError>),
    Boot { component: ComponentId, source: ComponentError, rolled_back: Vec<ComponentId> },
    Run(Vec<RunError>),
    Shutdown(Vec<ShutdownError>),
}
```

### Agrégation, pas premier échec

En phases 1, 2, 3 : **toutes** les erreurs sont collectées et rapportées
ensemble. Un démarrage qui révèle une erreur à la fois, six fois de suite, est
un défaut d'outillage.

### Le Kernel ne connaît aucune erreur de domaine

Un Bundle remonte son erreur sous forme
`Box<dyn std::error::Error + Send + Sync + 'static>`, que le Kernel enveloppe
avec l'identité du bundle/component et la phase. Le Kernel n'a jamais à nommer
un type d'erreur métier — c'est la même règle d'altitude qu'au § 16.

### Table de fatalité

| Où | Conséquence |
|---|---|
| Configure / Register / Resolve | Le Kernel ne démarre pas. Rien n'a été instancié. |
| Boot | **Rollback** : les Components déjà bootés sont arrêtés en ordre inverse réel, puis sortie non nulle. |
| Run, Runnable `Essential` | Phase 6 déclenchée, arrêt propre, sortie non nulle. |
| Run, Runnable `Ancillary` | Politique de redémarrage ; épuisée → journalisé, le Kernel continue. |
| Shutdown | Journalisé, n'empêche aucun autre arrêt, influe sur le code de sortie. |

### Sortie

```rust
pub enum Outcome {
    Completed,          // every runnable finished on its own
    ShutdownRequested,  // signal or programmatic
    Failed(KernelError),
}
```

---

## 13. Arrêt

### Deux temps, pas un

```
Running → Draining → Stopping → Stopped
```

- **Draining** : on cesse d'accepter du travail neuf, le travail en vol continue.
  Un serveur ferme son écoute, un consommateur cesse de tirer des messages.
- **Stopping** : le travail en vol doit se terminer, la limite de temps court.

Sans cette séparation, tout arrêt propre est impossible : un Runnable ne peut
pas à la fois refuser du neuf et finir l'ancien s'il n'a qu'un seul signal.

```rust
pub struct Shutdown { /* cheap to clone */ }

impl Shutdown {
    pub fn stage(&self) -> Stage;
    pub async fn draining(&self);   // resolves when Draining starts
    pub async fn stopping(&self);   // resolves when Stopping starts
    pub fn deadline(&self) -> Option<Instant>;
}
```

### Ordre et délais

- Ordre d'arrêt = **inverse exact de l'ordre de boot réellement observé**, pas
  l'inverse de l'ordre calculé. Si le boot a divergé (Component sauté, échec
  partiel), l'arrêt suit le fait, pas le plan.
- Runnables d'abord, Components ensuite.
- `ShutdownPolicy { drain: Duration, stop: Duration }` global, surchargeable par
  Component/Runnable via son descripteur.
- Dépassement du délai : la tâche est abandonnée, l'événement est enregistré, le
  Kernel continue son arrêt. **Jamais** de blocage indéfini.

### Déclencheurs

Signal du système (activable/désactivable au builder), terminaison d'un Runnable
`Essential`, appel de `KernelHandle::shutdown()`. `KernelHandle` est clonable et
résoluble depuis le Container : n'importe quel Component peut demander l'arrêt.

---

## 14. Observabilité et santé

Le Kernel a besoin d'émettre, mais ne doit dépendre d'aucune implémentation.

```rust
// contract defined by the kernel, implemented by a bundle
pub trait Telemetry: Send + Sync + 'static {
    fn record(&self, record: Record);
}
```

Le Kernel fournit deux implémentations triviales : `NoopTelemetry` et une sortie
texte sur la sortie d'erreur standard. Toute implémentation réelle est un
Bundle. Chaque transition de phase, chaque boot de Component, chaque
redémarrage de Runnable, chaque erreur produit un `Record`.

Santé :

```rust
pub trait HealthProbe: Extension {
    fn name(&self) -> &'static str;
    fn check<'a>(&'a self) -> BoxFuture<'a, Health>;
}
```

`HealthProbe` est un `ExtensionPoint` déclaré par le Kernel. Le Kernel agrège,
le Kernel n'expose pas : servir l'état de santé sur un port est le travail d'un
Bundle.

---

## 15. Décisions Rust — les points qui décident si ça compile

### Traits utilisables dynamiquement

Toutes les surfaces du Kernel sont dyn-compatibles. Les méthodes asynchrones
retournent un futur boxé via un alias défini par le Kernel :

```rust
pub type BoxFuture<'a, T> = core::pin::Pin<Box<dyn core::future::Future<Output = T> + Send + 'a>>;
```

Coût : une allocation par appel, aux **frontières de lifecycle uniquement**
(boot, shutdown, build, dispatch). Ces appels se comptent par dizaines sur la
vie du processus, pas par millions. Aucun chemin chaud applicatif ne traverse
une surface du Kernel : une fois résolu, l'appelant tient un `Arc<dyn Contract>`
et appelle directement.

### Contraintes universelles

- Tout ce que le Kernel conserve : `Send + Sync + 'static`.
- Aucun paramètre de durée de vie ne traverse une frontière de Bundle.
- Aucun paramètre générique dans les types **stockés** ; les génériques ne
  vivent qu'au site d'appel (`get::<dyn X>()`), donc aucune contamination
  générique entre crates.
- Récepteur `self: Arc<Self>` pour `Runnable::run` — dyn-compatible, et donne au
  Runnable la propriété partagée dont il a besoin pour être détaché.

### Macros

Aucune macro n'est requise. Chaque macro fournie se développe en une API
publique que l'utilisateur pourrait écrire à la main. Conséquence testable : le
Kernel doit compiler et fonctionner avec le crate de macros retiré.

Macros fournies : dérivation de `FromConfig`, sucre de déclaration de
`Provider` (génère `requires` depuis la signature du constructeur), sucre de
`Listener`.

### Découpage des crates du Kernel

```
kernel/
  crates/
    kernel-core/       # traits, ids, errors, ConfigTree, BoxFuture — aucune dépendance runtime
    kernel/            # Container, Registry, Dispatcher, Supervisor, Kernel — runtime async
    kernel-macros/     # dérivations, optionnel
    kernel-testkit/    # builder de test, doublures
```

Raison du découpage : une crate `*-contracts` ne dépend que de `kernel-core`,
donc elle est légère, stable, et compilable sans runtime. C'est ce qui rend la
règle d'isolation du § 3 supportable en pratique.

---

## 16. Frontière Kernel / Bundle et gardes

### Liste fermée de ce qui est DANS le Kernel

Phases et machine à états · Registry · Container · Contracts (identité et
résolution) · Component · Service (fourniture) · Runnable et supervision ·
ExtensionPoint · EventDispatcher/Listener · arbre de configuration et source
d'environnement · modèle d'erreur · coordination d'arrêt · contrats `Telemetry`
et `HealthProbe` (définitions seules) · harnais de test.

### Tout le reste est un Bundle

HTTP · base de données · analyse de ligne de commande · planification ·
export de métriques · implémentation de journalisation · authentification ·
sérialisation de fichiers de configuration · et **tout** ce qui est métier.

### Règle d'altitude

> Aucun type du Kernel ne nomme une entité de domaine, ni un transport, ni une
> technologie. Un tel nom dans `kernel-core` ou `kernel` est un défaut, pas un
> raccourci.

### Gardes — une frontière énoncée mais non vérifiée n'existe pas

| Frontière | Garde |
|---|---|
| Le Kernel ne dépend de rien de métier | Liste blanche de dépendances vérifiée en CI sur `kernel-core` et `kernel` |
| Aucun nom de domaine dans le Kernel | `grep` en CI sur un glossaire de domaine du projet |
| Bundle ↔ Bundle interdit | Vérification en CI : aucune crate `*-bundle` dans les dépendances d'une crate `*-bundle` |
| `requires` conforme à `build` | Instrumentation du Container en build de debug, échec si résolution non déclarée |
| Pas de résolution paresseuse en phase 5 | Le Container refuse une première instanciation `Shared` après `BootCompleted` |
| Macros non obligatoires | Une tâche de CI compile la suite sans `kernel-macros` |
| Surface publique documentée | `#![deny(missing_docs)]` sur `kernel-core` et `kernel` |
| Aucune dépendance de production n'allume `kernel/testing` | `ci/check-testing-feature.sh` : arbre résolu, table de dépendances déclarée, table de features déclarée |

---

## 17. Anatomie d'un Bundle — couche application

> Cette section est une **illustration**. Rien de ce qui suit n'appartient au
> Kernel, et aucun de ces noms n'apparaît dans une surface du Kernel.

```
storage/
  crates/
    storage-contracts/          # pub trait Storage — dépend de kernel-core
    storage-component/          # StorageComponent : possède le pool, implémente Component
    storage-bundle/             # implémente Bundle : provide::<dyn Storage>, config, sondes

orders/
  crates/
    orders-contracts/
    orders-bundle/              # dépend de storage-contracts, JAMAIS de storage-bundle
      src/
        services/               # convention applicative, pas un type du Kernel
        handlers/               # contributions à un ExtensionPoint
        listeners/
```

Point d'entrée :

```rust
#[tokio::main]
async fn main() -> ExitCode {
    // `?` n'est pas utilisable ici : `ExitCode` n'implémente pas `FromResidual`.
    // L'échec de construction se rend donc explicitement.
    let kernel = match Kernel::builder()
        .config_source(defaults())
        .config_source(EnvSource::with_prefix("APP_"))
        .bundle(StorageBundle::default())
        .bundle(OrdersBundle::default())
        .build()          // phases 1 → 3, aucune E/S, aucune instanciation
        .await
    {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    let outcome = kernel.run().await;     // phases 4 → 7
    if let Some(error) = outcome.error() {
        eprintln!("{error}");
    }
    outcome.into_exit_code()
}
```

`build()` est la barrière : s'il rend `Ok`, le graphe est valide, la
configuration est valide, tous les contrats sont satisfaits. Rien n'a encore
tourné.

---

## 18. Test

- `TestKernelBuilder` : mêmes sept phases, pas de capture de signal, budgets
  d'arrêt courts, et un enregistreur de telemetry accessible avant la
  construction. `TestHarness` conduit le Kernel depuis un test : `start`,
  `stop`, `wait`.
- La substitution de liaison vit sur **`TestBuilder`**, un type distinct de
  `KernelBuilder`. La garantie tenue est celle du **graphe de production** : un
  graphe qui atteint `kernel` sans passer par une dev-dependency sur
  `kernel-testkit` compile avec la feature `testing` éteinte, `__register_hook`
  n'y existe pas, et aucune substitution n'y est atteignable. C'est
  `ci/check-testing-feature.sh` qui la tient — pas le type.
- La garantie plus forte — « quelles que soient les features actives dans le
  graphe de compilation » — **n'est pas atteignable entre crates en Rust**, et
  l'affirmer était faux. Une feature Cargo est unifiée sur tout un build : dans
  le `cargo test` d'un crate qui dev-dépend de `kernel-testkit`, `kernel/testing`
  est allumée pour l'ensemble du graphe, et n'importe quel `#[test]` de ce crate
  peut appeler `KernelBuilder::new().__register_hook(...)` sans nommer un seul
  type de `kernel-testkit`. Vérifié par expérience, pas supposé. Rien ne permet
  de restreindre une feature au graphe de dev d'un seul crate.
- Ce résidu est **accepté** : qui atteint le hook est en train d'écrire un test.
  Passer par `TestBuilder` reste ce qui place la substitution dans l'ordre des
  phases — après tous les `register`, devant la validation de phase 3 — et c'est
  la seule chose que le type gouverne.
- Une substitution **conserve la nature** de ce qu'elle remplace : la doublure
  d'un Component reste bootée par le Kernel.
- Un Bundle peut être booté seul. Les contrats non satisfaits sont rapportés en
  phase 3 sous forme de liste — cette liste **est** exactement la liste des
  doublures à écrire.
- Une substitution **remplace ou ajoute** : un contrat qu'un Bundle du graphe
  lie déjà est remplacé — la doublure prend sa place, son rang et sa position
  par défaut — et un contrat que personne ne lie est ajouté. Le remplacement
  passe par des affordances `#[doc(hidden)]` du `Registry`, sous la même feature
  `testing` que le hook, atteintes depuis lui : ce n'est pas un huitième verbe,
  la liste fermée de sept étant la surface offerte aux Bundles.
- `FnBundle` (un Bundle depuis une fermeture) vit dans `kernel` : une
  application qui veut un seul Listener n'a pas à écrire un type et deux
  méthodes de trait pour atteindre le formulaire. `kernel-testkit` le
  ré-exporte, et fournit `EventLog<E>` (un Listener qui conserve chaque
  événement de type `E`) et `missing_contracts`. L'enregistreur de telemetry, `RecordingTelemetry`, vit
  dans `kernel-core` parce que le Kernel s'en sert aussi comme implémentation
  par défaut de diagnostic. `missing_contracts` rend un
  `Result<Vec<ContractRef>, KernelError>` : la liste vide dit « ce Bundle tient
  seul », l'erreur dit « il n'a pas atteint la phase 3 ». Il construit **sans
  aucune source de configuration**, donc un Bundle qui lit sa configuration en
  `register` échoue là et le dit, au lieu de se faire passer pour autonome.
- Un Runnable est testable hors Kernel : on lui passe un `RunContext` fabriqué
  et on vérifie qu'il rend la main sur le jeton d'arrêt. Ce test est **exigible
  pour tout Runnable**.

---

## 19. Hors périmètre de la première version — validé

Ces huit exclusions ont été posées comme questions et **validées**. Elles sont
donc des décisions, au même titre que celles des sections précédentes. Le code
livré les respecte toutes.

| # | Exclusion actée | Conséquence dans le code |
|---|---|---|
| 1 | **Rechargement de configuration à chaud** | L'arbre est figé après la phase 1 ; `Config` est une valeur immuable, liée dans le conteneur |
| 2 | **Chargement dynamique de Bundles** | Les Bundles sont liés à la compilation ; `KernelBuilder::bundle` prend une valeur, pas un chemin |
| 3 | **Kernels multiples ou imbriqués dans un processus** | Rien n'interdit techniquement deux `Kernel`, mais aucune coordination entre eux n'est fournie ni testée |
| 4 | **Événements franchissant la frontière du processus** | Le dispatcher est en mémoire, sans persistance ni rejeu ; relayer est le travail d'un Bundle |
| 5 | **Scopes imbriqués ou nommés** | `Container::scope()` ouvre un enfant à un seul niveau ; appelé sur un scope, il rend **la même** unité de travail plutôt qu'un frère silencieux |
| 6 | **Redémarrage d'un Component** | `RestartPolicy` ne s'applique qu'aux Runnables ; un Component boote une fois |
| 7 | **Pont de sérialisation intégré** | `FromConfig` est un trait maison avec sa dérivation ; aucun pont n'est fourni |
| 8 | **Métriques natives** | Le Kernel n'expose que `Telemetry::record` ; toute agrégation est un Bundle |

Chacune peut être rouverte plus tard sans casser une surface publique existante :
les points 1, 4, 7 et 8 s'ajoutent par extension, les points 2, 3, 5 et 6
demanderaient une décision d'architecture nouvelle.

---

## 20. Récapitulatif des décisions

| # | Sujet | Décision |
|---|---|---|
| 1 | Runtime | Le Kernel ne le possède pas ; il est une `Future` |
| 2 | Phases | Sept, ordre strict, aucune sautée |
| 3 | Register / Resolve | Deux phases séparées ; `register` est sync et aveugle |
| 4 | Container | Résolution dynamique, effacement de type, validée en phase 3 |
| 5 | Résolution paresseuse | Interdite après le boot, refusée par le Container |
| 6 | Contracts | Traits en crate séparée ; seul canal entre Bundles |
| 7 | Bundle ↔ Bundle | Interdit, gardé en CI |
| 8 | Component vs Service | Lifecycle géré ou non — critère unique |
| 9 | Types de Component | Un seul trait ; l'enveloppe et le natif ne diffèrent que par ce qu'ils possèdent |
| 10 | Ce qui tourne | `Runnable`, supervisé, avec criticité et politique de redémarrage |
| 11 | Controller / Command | Absents du Kernel ; remplacés par `ExtensionPoint` |
| 12 | Événements | Typés statiquement ; `dispatch` séquentiel vs `emit` détaché |
| 13 | Listeners | Enregistrés en phase 2 uniquement ; table immuable ensuite |
| 14 | Lifecycle du Kernel | Émis comme événements de première classe |
| 15 | Configuration | Arbre du Kernel, sources injectées, typée et validée en phase 2 |
| 16 | Erreurs | Une variante par phase, agrégées, jamais de type de domaine |
| 17 | Arrêt | Deux temps (`Draining`, `Stopping`), inverse du boot **réel**, délais bornés |
| 18 | Observabilité | Contrat `Telemetry` défini par le Kernel, implémenté par un Bundle |
| 19 | Rust async | `BoxFuture` partout aux frontières ; `Send + Sync + 'static` universel |
| 20 | Macros | Aucune n'est requise ; vérifié par une tâche de CI |
