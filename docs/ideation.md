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

### Le Kernel ne crée pas le runtime

**Décision.** `Kernel::run()` est une `Future`. L'application garde son `main`,
crée le runtime et choisit sa configuration de threads.

**Le runtime, lui, est Tokio.** La crate `kernel` en dépend sans option (`rt`,
`time`, `sync`, `macros`) et `run()` conduit ses phases sur une tâche
`tokio::spawn` : appelée hors d'un runtime Tokio, elle panique. Ce qui est acquis
est donc plus étroit que « aucun runtime » — le Kernel n'en **crée** aucun, n'a
aucune opinion sur le nombre de threads, et tourne sur un runtime
`current_thread` aussi bien que sur un multi-threads.

Conséquences acquises : les tests démarrent un Kernel dans un test async
standard ; un Kernel peut être embarqué dans une application qui a déjà son
propre `main`.

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
| **Service** | Unité **sans** lifecycle | Oui s'il est `Shared` : son instance est dans la table dès la phase 4 — mais le Kernel ne lui appelle ni `boot` ni `shutdown` |
| **Runnable** | Tâche longue durée supervisée | Oui |
| **ExtensionPoint** | Collection typée alimentée par plusieurs Bundles | Oui, par identité |
| **EventDispatcher / Listener** | Diffusion d'événements typés | Oui |

La liste est fermée sur les **concepts d'architecture**, pas sur la surface
publique : `Provider` et `Binding`, `Scope` (§ 5), `Extension` — l'objet
contribué, distinct du point (§ 9) —, `Telemetry`, `Probe`, `HealthReport`
(§ 14), `Outcome` (§ 12), `KernelHandle`, `Shutdown`, `Tick` (§ 13) sont exportés
par `kernel` : ce sont les mécanismes de ces dix concepts, pas des concepts de
plus.

Ce qui **n'est pas** dans cette liste et n'y entrera pas : `Controller`,
`Command`, `Route`, `Migration`, `Job`, `Middleware`. Ce sont des concepts de
transport ou de domaine ; ils vivent dans les composants qui les définissent, et
le Kernel leur fournit le seul mécanisme dont ils ont besoin : `ExtensionPoint`
(§ 9). Voir la règle d'altitude, § 16.

---

## 2. Lifecycle — sept phases, ordre strict, aucune n'est contournée

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

Aucune phase n'est contournable ni facultative — mais un échec **termine** la
séquence : un échec de phase 4 rend un `Outcome::Failed` depuis la phase 4
elle-même, le rollback tenant lieu d'arrêt, et les phases 5 et 6 n'ont pas lieu.
La colonne « Échec = » dit où chaque chemin s'arrête.

### La règle qui tient tout : rien ne se résout tard

> **Toute erreur de graphe (contrat manquant, cycle, ambiguïté, config invalide)
> doit apparaître au plus tard en phase 3.** Ce qui est interdit ensuite, c'est
> la découverte tardive — pas la construction tardive.

C'est le prix — et la contrepartie — de la résolution dynamique (§ 5). Une
résolution qui échouerait à la première requête HTTP en production est un défaut
de conception, pas une erreur d'exploitation.

**Le sceau, vérifié.** Aucune liaison `Shared` n'est instanciée après le boot :
la phase 4 instancie **toute** la table `Shared` — pas seulement la part que le
plan de boot atteint — avant le premier `boot`, puis **scelle** le conteneur
(`Container::seal` et `is_sealed` sont publics) ; passé le sceau, une première
instanciation `Shared` est refusée par `ContainerError::Sealed`. Le balayage
complet est ce qui rend le sceau juste plutôt que brutal : une liaison `Shared`
atteinte seulement depuis le `requires` d'un `Factory` n'est nommée par aucune
unité, un boot piloté par le plan la sauterait, et la première unité de travail
se la verrait refuser. Une valeur `Scoped` ou `Factory`, elle, est construite à la
demande par conception : ce qu'elle résout a été validé en phase 3 comme le reste
du graphe, donc rien ne s'y découvre.

**Les déclarations, gardées en debug.** En `debug_assertions`, le conteneur remis
à un `build`, à un `BootContext` et à un `RunContext` porte le `requires` de la
liaison correspondante et panique sur toute résolution hors déclaration. Elle
n'est pas totale : une résolution conditionnelle n'est vue que sur les exécutions
qui l'atteignent, un scope n'en porte pas — une unité de travail résout du
`Scoped`, qu'un appelant non `Scoped` ne peut pas déclarer sans que la phase 3 le
refuse (`LifetimeConflict`) — et un build de release n'en porte aucune.

### Register et Resolve sont deux phases, pas une

L'ébauche initiale écrivait : « lors de l'enregistrement, le bundle enregistre tout ce
qu'il doit enregistrer **et charge tout ce dont il a besoin** ». Impossible :
quand le Bundle A s'enregistre, le Bundle B n'existe pas encore.

**Décision.** `Bundle::register` est **synchrone**, ne reçoit **pas** le
Container, et n'a **aucun** moyen d'atteindre un autre Bundle. Il ne fait que
déclarer. La consommation n'a lieu qu'en phase 4, via des fermetures que le
Container appellera lui-même.

Deux précisions. `KernelBuilder::build` est déclarée `async` bien que les phases
1 à 3 ne fassent rien d'asynchrone — son corps ne contient aucun `await`. Et les
notifications de la phase 2 (`BundleRegistered`, une par Bundle, puis
`GraphResolved`) sont publiées à la **fin** de `build`, le dispatcher n'existant
qu'une fois la phase 3 passée : un Listener peut tourner avant la phase 4.

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

Un Bundle **publié** = une crate. Une feature **distribuée** = un workspace :

```
my-feature/                       # workspace de distribution
  crates/
    my-feature-contracts/         # traits publics uniquement — dépend de kernel-core
    my-feature-component/         # optionnel : le bloc technique réutilisable
    my-feature-bundle/            # la crate qui implémente Bundle
```

Un Bundle qui n'est pas distribué n'a pas besoin de crate : `FnBundle` fait un
Bundle d'une fermeture, et c'est la forme que prend le bundle propre à
l'application — celui qui enregistre un Listener ou un Component d'affichage sans
rien publier. La règle de crate porte sur ce qui est distribué.

**Règle d'isolation, et c'est la principale du système :**

> Une crate `*-bundle` ne figure JAMAIS dans les dépendances d'une autre crate
> `*-bundle`. Elle atteint une autre feature par sa crate `*-contracts`, et par
> rien d'autre.

La **première** phrase est celle qui est **gardée en CI** (§ 16) :
`ci/check-bundle-graph.sh` marche le graphe résolu et refuse toute arête
`*-bundle` → `*-bundle`. La seconde reste une règle de conception non gardée, et
elle porte sur les *autres* features : une crate `*-bundle` dépend aussi du
Kernel et, le cas échéant, de la crate `*-component` de sa propre feature.

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

« Trait » est la forme recommandée, pas la forme imposée : le Container accepte
comme clé n'importe quel type `?Sized + Send + Sync + 'static`, et le Kernel s'en
sert lui-même — `registry.component::<T>()` lie le type **concret** de l'unité
(§ 7).

C'est le seul moyen pour un Bundle d'atteindre par **résolution** une capacité
fournie par un autre Bundle. Ce n'est pas « si besoin » : c'est le mécanisme
central de découplage. Deux autres canaux traversent la frontière sans rien
résoudre : la contribution à un `ExtensionPoint` (§ 9) et l'événement (§ 10) —
tous deux typés, eux aussi, par une crate `*-contracts`.

```rust
// in my-feature-contracts
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}
```

- **Identité** : `ContractId { type_id: TypeId::of::<dyn Clock>(), name: Option<&'static str> }`,
  la clé de hachage. Les signatures publiques prennent une `ContractRef` : même
  identité, mais construite en `const` et porteuse du `type_name`, ce qui permet
  à `requires` d'être un tableau statique et aux diagnostics de nommer le
  contrat au lieu de l'afficher en `TypeId`.
- **Fourniture** : `registry.provide::<dyn Clock>(provider)`.
- **Consommation** : `container.get::<dyn Clock>().await -> Result<Arc<dyn Clock>, ContainerError>`.
  La résolution est asynchrone et faillible ; `get_named` et `get_all` le sont
  aussi. Un provider qui résout dans son `build` convertit l'erreur lui-même :
  `BuildError` n'implémente pas `From<ContainerError>`.
- **Vérification** : phase 3, **toutes les violations agrégées**, jamais la
  première seule. Dix contrôles : `MissingContract`, `DuplicateDefault`,
  `DuplicateNamed`, `Cycle`, `UndeclaredExtensionPoint`, `ManifestMismatch`,
  `UnknownBundleOrder`, `BundleCycle`, `DuplicateBundle`, `LifetimeConflict`.
  Trois d'entre eux lisent aussi les Listeners — ce qu'un Listener résout pendant
  la diffusion est vérifié là ou nulle part.

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

`Shared` est le défaut, et toute la table `Shared` est instanciée en phase 4
(§ 2), y compris les liaisons que rien n'atteint. `Scope` est un container enfant
créé par celui qui possède l'unité de travail (typiquement un Component de
transport) ; il porte sa propre table et délègue au parent pour tout ce qui est
`Shared`. Les scopes **ne s'imbriquent pas** : `Container::scope()` appelé sur un
scope rend *le même* scope plutôt qu'un frère silencieux — une unité de travail
ouverte dans une unité de travail est la même unité de travail.

Résoudre une liaison `Scoped` hors d'un scope est refusé
(`ContainerError::NoScope`) plutôt que servi par une valeur construite pour
l'occasion que personne n'a demandée — et le cas ne surgit pas par surprise,
puisque la phase 3 refuse d'avance qu'une liaison non `Scoped`, ou un Listener
qui diffuse hors de tout scope, déclare un `requires` sur du `Scoped`
(`ResolveError::LifetimeConflict`).

### Fourniture

```rust
pub struct Provider<C: ?Sized + Send + Sync + 'static> {
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
let clock: Arc<dyn Clock> = erased
    .downcast_ref::<Arc<dyn Clock>>()
    .map(Arc::clone)
    .ok_or(ContainerError::TypeMismatch { contract })?;
```

Une indirection `Arc` supplémentaire, payée une fois par résolution, jamais dans
un chemin chaud. Toute la partie fragile est confinée dans **un seul module** du
Container, avec une surface auditée.

### Règles invariantes

- Tout ce que le Container conserve est `Send + Sync + 'static`.
- Le Container ne rend **jamais** de `&mut`. La mutabilité intérieure est le
  problème du fournisseur.
- Après la phase 4, la table `Shared` est **immuable** : le conteneur est scellé
  et refuse toute première instanciation `Shared`. Il n'y a **aucun verrou** :
  chaque case est allouée à la construction et écrite au plus une fois, donc une
  lecture après le sceau n'est en concurrence avec rien. Ce n'est de toute façon
  pas un chemin chaud applicatif, puisqu'un appelant qui a résolu tient un `Arc`
  et appelle directement.
- La table des liaisons **est** la table des unités : `registry.component::<T>()`
  et `registry.runnable::<T>()` lient `T` comme contrat en plus d'enregistrer
  l'unité (§ 6), si bien que `container.get::<MonComponent>()` rend l'objet même
  que le Kernel boote — et non un second construit à côté.
- Les cycles sont détectés en phase 3 par tri topologique et rapportés avec le
  cycle complet, pas avec un seul nœud.

---

## 6. Registry

Le cahier que les Bundles remplissent en phase 2. Surface unique et fermée — la
liste de ce qu'un Bundle peut enregistrer n'a **pas** de « etc. » :

```rust
impl Registry {
    // Contracts
    fn provide<C: ?Sized + Send + Sync + 'static>(&mut self, provider: Provider<C>) -> Binding<'_, C>;
    fn provide_named<C: ?Sized + Send + Sync + 'static>(&mut self, name: &'static str, provider: Provider<C>) -> Binding<'_, C>;

    // Lifecycle-managed units
    fn component<T: Component>(&mut self, provider: Provider<T>) -> Binding<'_, T>;

    // Long-running units
    fn runnable<T: Runnable>(&mut self, provider: Provider<T>) -> Binding<'_, T>;

    // Events
    fn listen<E: Event, L: Listener<E>>(&mut self, listener: L, priority: Priority) -> Listening<'_, E>;

    // Extension points
    fn declare_extension_point<X: Extension>(&mut self);
    fn contribute<X: Extension>(&mut self, item: X);
}
```

**Sept verbes d'enregistrement** — `provide`, `provide_named`, `component`,
`runnable`, `listen`, `declare_extension_point`, `contribute` — et rien d'autre
n'entre dans le Kernel par cette porte. Toute demande d'un huitième verbe est
une décision d'architecture, pas un ajout. (Le `Registry` porte aussi quatre
affordances de **substitution**, `#[doc(hidden)]` et sous la feature `testing` :
elles ne sont pas atteignables depuis un Bundle et sont décrites au § 18.)

Trois de ces verbes rendent une poignée qui sert à déclarer, jamais à
enregistrer autre chose. `provide` et `provide_named` rendent un `Binding` —
c'est lui qui porte `as_default`. `listen` rend un `Listening`, sur lequel
`requires` déclare ce que le Listener résoudra **pendant la diffusion** : la
phase 3 lit ces déclarations comme celles d'un provider, et ce qu'un Listener ne
déclare pas là n'est vérifié nulle part.

`component` et `runnable` font deux choses de plus que leur nom : ils **lient
`T` comme contrat** dans la même table (§ 5) et **forcent** `Lifetime::Shared` —
une unité que le Kernel boote une fois mais que le Container reconstruirait par
scope serait deux objets pour un nom. Une durée de vie demandée autrement est
écrasée, et l'écrasement est consigné (`registry.lifetime_overridden`, `Warn`)
plutôt qu'appliqué en silence.

S'y ajoutent deux **accesseurs** en lecture seule, qui n'enregistrent rien :

```rust
impl Registry {
    /// Typed read of a configuration section.
    fn config<T: FromConfig>(&self, prefix: &str) -> Result<T, ConfigError>;
    fn telemetry(&self) -> &Arc<dyn Telemetry>;
}
```

Ils donnent au Bundle ce dont il a besoin pour construire ses déclarations. Ils
prennent `&self`, ne touchent pas la table d'enregistrement, et ne font donc pas
partie de la surface fermée. `telemetry` rend un `&Arc` et non un `&dyn` : c'est
ce qui permet à une unité de **garder** le puits au-delà de l'appel, et les
contextes de boot, de run et de Listener rendent le même `&Arc`.

---

## 7. Component et Service — la distinction

L'ébauche initiale faisait cohabiter « components » et « services » sans les séparer.
La séparation retenue est **opérationnelle**, donc vérifiable :

> **Un Component a un lifecycle géré par le Kernel. Un Service n'en a pas.**

| | Component | Service |
|---|---|---|
| Reçoit `boot` / `shutdown` | Oui | Non |
| Possède une ressource (pool, socket, client) | Typiquement | Non |
| Instancié quand | Phase 4, ordre topologique | Phase 4 aussi s'il est `Shared` — toute la table est balayée (§ 2) ; à la résolution s'il est `Scoped` ou `Factory` |
| Porte un nom de lifecycle déclaré | Oui, `Component::name` : c'est lui que le plan de boot indexe et que la telemetry met en cause | Non |

Toute liaison, Service compris, est connue du Kernel **par identité** : un
`ContractId` et une `ContractRef` qui porte le nom de type affiché dans les
diagnostics de phase 3. Ce qu'un Component a en plus est un nom d'unité, déclaré.
Et ni l'un ni l'autre ne déclare un `ExtensionPoint` : `declare_extension_point`
est un verbe du `Registry`, donc du Bundle (§ 9).

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
validé), à la config, au dispatcher, à la telemetry, à un `KernelHandle` — c'est
par là qu'un Component demande l'arrêt depuis son `boot` — et aux
`ExtensionPoint` de deux façons : `collect`, qui prête les items le temps de
l'appel, et `extensions`, qui rend la table entière dans un `Arc` survivant à
l'appel. La seconde est ce dont a besoin un Component qui relira les
contributions à chaque requête, longtemps après son boot.

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
    /// Shortens this unit's own `Draining` budget; never extends it.
    pub drain_timeout: Option<Duration>,
    /// Shortens this unit's own `Stopping` budget; never extends it.
    pub stop_timeout: Option<Duration>,
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
3. Tous les Runnables démarrent **après** que tous les Components ont booté. Le
   Kernel n'en ordonne aucun contre un autre : un Runnable peut détenir l'`Arc`
   d'un autre, mais ne doit rien supposer du moment où celui-ci tourne.

### Ce que fait le superviseur

- lance tous les Runnables, garde leurs poignées ;
- attend le premier des événements : un Runnable `Essential` se termine (quel
  que soit le résultat), **tous** les Runnables se sont terminés d'eux-mêmes, un
  signal d'arrêt, un appel programmatique à `handle.shutdown()` ;
- applique la politique de redémarrage aux `Ancillary` en échec ;
- déclenche la phase 6.

Le deuxième cas est celui du Kernel en forme de traitement par lots :
`ShutdownReason::Completed`, rendu immédiatement s'il n'y a aucun Runnable du
tout — un graphe d'objets sans rien qui tourne est un programme déjà fini — et
c'est lui qui fait sortir un tel Kernel avec un code zéro (§ 12).

Deux détails du redémarrage : le délai de backoff est servi **dans** la tâche
relancée — un redémarrage en attente ne retient jamais le superviseur — et il est
abandonné dès que `Stopping` a commencé. Un `Ancillary` à bout de tentatives est
consigné (`runnable.restarts_exhausted`) et le Kernel continue sans lui.

**Échec au démarrage de la phase 5.** Le superviseur résout d'abord chaque
Runnable depuis la table `Shared` déjà instanciée ; un échec là défait les
Components en ordre inverse, ne publie **aucun** événement de lifecycle — un
Kernel qui n'a jamais atteint la phase 5 n'est jamais entré en phase 6 — et rend
`Failed(KernelError::Run(..))`.

---

## 9. ExtensionPoint — ce qui remplace « controllers » et « commands »

L'ébauche initiale listait `controllers/` et `commands/` comme des dossiers. Les
promouvoir en types du Kernel serait une faute d'altitude : `Controller`
présuppose un transport, `Command` présuppose une ligne de commande.

**Décision.** Le Kernel ne connaît ni l'un ni l'autre. Il fournit le seul
mécanisme générique dont ils ont besoin : **une collection typée qu'un Bundle
déclare, que n'importe quel Bundle alimente, et que lit quiconque tient le
Container.**

```rust
pub trait Extension: Send + Sync + 'static {}

// A bundle declares the point its unit will consume (the type lives in a
// *-contracts crate, so a contributor never depends on the declaring bundle)
registry.declare_extension_point::<HttpRoute>();

// Any bundle contributes to it
registry.contribute::<HttpRoute>(HttpRoute::new("/orders", handler));

// Anything holding the container collects; a component usually does it at boot
let routes: Vec<&HttpRoute> = cx.collect::<HttpRoute>();
```

Règles :

- contribuer à un point **non déclaré** est une erreur de phase 3 ;
- un point déclaré et vide est valide ;
- chaque contribution reçoit un rang d'enregistrement global, et la collecte
  rejoue ces rangs : l'ordre est donc l'ordre d'enregistrement des Bundles, et,
  à l'intérieur d'un Bundle, l'ordre des `contribute`. Déterministe et
  reproductible aux deux niveaux ;
- déclarer et collecter ne sont l'apanage d'aucune unité :
  `declare_extension_point` est un verbe du Bundle, et **quiconque tient le
  Container** collecte (`container.extensions()` rend la table, un Runnable peut
  donc la lire aussi bien qu'un Component). `ExtensionPoints` répond en outre à
  `is_declared::<X>()` et `count::<X>()`.

Le même mécanisme sert, sans code supplémentaire dans le Kernel : routes HTTP,
commandes CLI, tâches planifiées, migrations, intercepteurs. `HttpRoute`,
`CliCommand`, `Migration` sont définis dans les crates `*-contracts` de leurs
features — **jamais** par le Kernel.

Une seule exception, assumée et bornée : la santé. Le Kernel définit le type
`Probe` et déclare lui-même ce point, parce qu'un point est indexé par le type
contribué et qu'il faut un type commun pour que l'agrégat les atteigne tous
(§ 14).

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
    /// Sequential, awaited, priority-ordered. Listeners may mutate the event
    /// and stop propagation. The first failure ends the walk — the listeners
    /// after it do not run — and reaches the emitter.
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<Dispatched, DispatchError>;

    /// Detached. Ordering not guaranteed. Errors are reported to telemetry only.
    pub fn emit<E: Event>(&self, event: E);

    /// Resolves once every detached walk has finished, the ones started while
    /// waiting included. `in_flight` samples what is still outstanding.
    pub async fn settle(&self);
}
```

`dispatch` sert quand le flot de contrôle de l'émetteur dépend du résultat
(enrichissement, veto, validation en chaîne). `emit` sert à la notification.
Confondre les deux est la principale source de bugs dans ce genre de mécanisme,
d'où deux méthodes et non un booléen. `Dispatched` rapporte le nombre de
Listeners parcourus et si l'un a arrêté la propagation ; `listener_count::<E>()`
dit combien la table en porte.

**`emit` est détaché, pas perdu.** Chaque émission prend un jeton et `settle()`
résout quand plus aucune marche n'est en cours ; le Kernel l'attend **deux fois**
sur l'échelle d'arrêt — avant que les Components ne descendent, puis après le
dernier événement du run —, chaque attente bornée par `stop`. C'est ce qui fait
qu'un Listener qui résout un Component n'en reçoit pas un déjà arrêté, et qu'une
notification émise juste avant l'arrêt arrive au lieu de courir contre la fin du
processus. `emit` peut en revanche **jeter** l'événement : sans runtime capté à
la construction du dispatcher, ou avant que la phase 3 ne lui attache le
Container, il est consigné en `Error` (`dispatcher.dropped`) et abandonné — une
notification ne fait pas tomber un processus. Symétriquement, `dispatch` avant la
phase 3 rend un `DispatchError`.

### Enregistrement figé

Les Listeners ne s'enregistrent **qu'en phase 2**, par `registry.listen`, dont la
poignée `Listening` sert à déclarer ce que le Listener résoudra pendant la
diffusion (§ 6) — la phase 3 le vérifie comme le `requires` d'un provider. Après
la phase 3, la table est immuable, donc lisible sans verrou pendant toute la
phase 5. Aucun enregistrement dynamique de Listener n'est prévu.

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

La colonne « Phase » nomme la phase que l'événement **rapporte**, pas l'instant
où il part : les deux premiers sont publiés à la fin de `build`, le dispatcher
n'existant qu'à partir de la phase 3 (§ 2). Deux charges utiles portent une
décision plutôt qu'un compte : `ShutdownReason` a quatre variantes — `Signal`,
`Programmatic`, `EssentialFinished`, `Completed` — et `Stopped` porte trois
comptes distincts, `abandoned` (délai dépassé), `unhandled` (ce dont rien ne
s'est remis, zéro sur toute sortie réussie) et `run_failures` (toute fin
anormale, redémarrages compris).

### Frontière

Le dispatcher est **en mémoire, intra-processus, sans persistance**. Faire
franchir une frontière de processus à un événement est le travail d'un Bundle
qui écoute et relaie. Le Kernel reste agnostique du transport.

---

## 11. Configuration

Absente de l'ébauche initiale, indispensable : elle conditionne la phase 2.

### Chaînage

```
source 1 → source 2 → … → source n        (le dernier gagne)
```

**Une seule chaîne ordonnée**, et un seul verbe pour la remplir :
`KernelBuilder::config_source`, qui empile dans un `ConfigChain`. « Défauts » et
« surcharges » ne sont pas des étages du mécanisme mais des **positions** : le
défaut est la première source poussée, la surcharge la dernière. La fusion est
**par feuille**, pas par bloc — surcharger une clé ne détruit pas ses voisines —
et la chaîne collecte l'erreur de toutes les sources qui échouent avant de
refuser.

### Le Kernel n'impose aucun format

```rust
pub trait ConfigSource: Send + Sync + 'static {
    /// Stable name, used in errors and diagnostics.
    fn name(&self) -> &'static str;

    fn load(&self) -> Result<ConfigTree, ConfigError>;
}
```

`ConfigTree` est une structure de données du Kernel : arbre de `Map` / `Seq` /
`Scalar`, écrit par `insert(path, node)` — qui refuse un index de séquence non
numérique ou hors bornes, et refuse d'entrer dans un scalaire. Le Kernel embarque
**deux** sources seulement : `EnvSource` (variables d'environnement) et
`MemorySource` (arbre en mémoire, `named` pour que l'erreur dise laquelle). Tout
format de fichier est fourni par l'application ou par un Bundle. Le Kernel ne
dépend d'aucune bibliothèque de sérialisation.

Le **mappage de l'environnement est un contrat visible**, donc fixé règle par
règle : préfixe retiré puis minuscules ; `__` sépare deux segments, un `_` seul
est un caractère ordinaire (`APP_SERVER__MAX_RETRIES` → `server.max_retries`) ;
valeur lue comme booléen, sinon `i64`, sinon `f64`, sinon chaîne ; variables
appliquées dans l'ordre trié de leurs noms, pour ne pas dépendre de celui du
système. `EnvSource::all()` prend tout ; `EnvSource::from_pairs` lit une liste
explicite — la couture qui garde un test hors de l'environnement du processus.

### Typage par Bundle

```rust
pub trait FromConfig: Sized {
    fn from_config(node: &ConfigNode) -> Result<Self, ConfigError>;
}

// in Bundle::register
let cfg: HttpConfig = registry.config::<HttpConfig>("http")?;
```

Le trait n'est utilisable que parce que `kernel-core` en fournit les
implémentations de base : `bool`, les entiers, `f32`/`f64`, `String`,
`ConfigNode`, `Option<T>`, `Vec<T>`, `BTreeMap<String, T>`, `Duration`, et
`Secret<T>` — qui rend un secret lisible directement depuis l'arbre.

Règle de style, pas loi du Kernel : **typez votre configuration en phase 2**, ce
qu'on type tard échoue tard. Rien ne l'impose — `BootContext::config()` et
`ShutdownContext::config()` existent, les deux exemples livrés s'en servent — et
retirer cette capacité pour acheter un absolu qu'aucune garde ne tiendrait serait
un mauvais échange.

Trade-off assumé : `FromConfig` est un trait maison, donc un dérive maison —
livré : `#[derive(FromConfig)]` et son attribut `#[config]`, dans
`kernel-macros` — plutôt qu'une dépendance de sérialisation imposée à toutes les
crates `*-contracts`. Un pont vers une bibliothèque de sérialisation est possible
derrière un drapeau de compilation, à la charge de l'application.

### Secrets

```rust
pub struct Secret<T>(T);
```

`Debug` et `Display` sont rédigés. L'accès à la valeur passe par une méthode
nommée explicitement. Coût nul, supprime la fuite la plus fréquente.

### Rechargement à chaud

Non prévu, et l'exclusion est actée (§ 19, exclusion 1).

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
un défaut d'outillage. Deux bornes à cette règle, et elles sont dans le code :
la phase 2 collecte **une** erreur par Bundle, `register` rendant un
`Result<(), RegisterError>` — un Bundle avec six chemins de config faux en
rapporte un ; et la **première phase qui échoue arrête la séquence**, parce qu'un
graphe résolu sur une configuration qui n'a pas chargé ne produirait qu'une
deuxième vague d'erreurs enterrant la première.

Les fins d'un Runnable sont distinguées par `RunErrorKind` — `Failed`,
`Panicked`, `Cancelled`, `DeadlineExceeded` : un `panic` est intercepté à la
jointure et devient un `RunError` au lieu de remonter dans le Kernel. Côté arrêt,
`ShutdownErrorKind { Failed, DeadlineExceeded }`.

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
| Run, résolution d'un Runnable | La phase 5 n'a pas lieu : rollback des Components, `Outcome::Failed(Run)`, aucun événement de lifecycle publié. |
| Run, Runnable `Essential` | Phase 6 déclenchée, arrêt propre. Sortie non nulle **sauf** si tous les autres Runnables étaient déjà terminés et que celui-ci rend `Ok` : c'est le Kernel en forme de lot, `Outcome::Completed`, sortie zéro. Terminé proprement mais pendant que d'autres tournaient encore : échec. |
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

`Outcome::is_success` et `Outcome::into_exit_code` sont ce qui rend « sortie non
nulle » opérationnel : `Failed` devient `ExitCode::FAILURE`, `Completed` et
`ShutdownRequested` `ExitCode::SUCCESS`.

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
    pub fn is_shutting_down(&self) -> bool;
    pub async fn draining(&self);   // resolves when Draining starts
    pub async fn stopping(&self);   // resolves when Stopping starts
    pub fn deadline(&self) -> Option<Instant>;

    /// Sleeps for `period`, or returns early if the ladder moved — `Tick` says
    /// which. The periodic-work primitive: a unit that polls needs no timer.
    pub async fn sleep_until_draining(&self, period: Duration) -> Tick;
    pub async fn sleep_until_stopping(&self, period: Duration) -> Tick;
}
```

### Ordre et délais

- Ordre d'arrêt = **inverse exact de l'ordre de boot réellement observé**, pas
  l'inverse de l'ordre calculé. Si le boot a divergé (Component sauté, échec
  partiel), l'arrêt suit le fait, pas le plan.
- Runnables d'abord, Components ensuite.
- `ShutdownPolicy { drain, stop }` est global mais **par unité**, pas par phase :
  ce sont les budgets qu'**une** unité reçoit, accordés à neuf quand son propre
  arrêt commence, et un descripteur ne peut que les raccourcir. Les Runnables
  s'arrêtant en parallèle, leur moitié coûte `drain + stop` quel qu'en soit le
  nombre ; les Components sont arrêtés l'un après l'autre. Pire cas :
  `drain + stop + (stop × components) + 2 × stop`, les deux derniers termes étant
  les attentes de `settle` (§ 10). Ce prix achète une règle — une unité n'est
  jamais abandonnée parce qu'une autre a débordé.
- **Les Components n'ont pas d'étape `Draining`** : leur marche ouvre une seconde
  échelle placée d'emblée en `Stopping`, rien de leur travail n'étant en vol.
  L'échelle en deux temps est celle des Runnables — d'où un seul
  `shutdown_timeout` sur `ComponentDescriptor` contre deux bornes sur
  `RunnableDescriptor`.
- Dépassement du délai : la tâche est abandonnée, l'événement est enregistré, le
  Kernel continue son arrêt. **Jamais** de blocage indéfini pendant l'arrêt.
- **L'attente qui suit `run()` n'est pas celle du Kernel.** Abandonner, c'est
  `abort()` sans attendre — et une annulation ne s'observe qu'à un point
  d'attente. Une tâche qui n'en atteint aucun continue donc d'occuper un thread
  de travail, et le `Drop` d'un `Runtime` multi-thread joint ces threads sans
  aucune borne. Le Kernel ne crée pas le runtime (§ 20, décision 1) et ne peut
  pas le détruire : c'est à l'application de borner cette dernière attente avec
  `Runtime::shutdown_timeout`, faute de quoi l'échelle se termine, annonce un
  `Outcome` réussi, et le processus ne sort jamais. Les deux exemples finissent
  ainsi, et `crates/kernel/tests/exit.rs` tient la borne. Le
  `boot`, lui, est non borné par défaut (`boot_timeout` vaut `None`) : un
  Component qui n'en revient jamais tient la phase 4 indéfiniment, et c'est à lui
  de déclarer sa borne.

### Déclencheurs

Quatre : signal du système (activable/désactivable au builder), terminaison d'un
Runnable `Essential`, appel de `KernelHandle::shutdown()`, et **abandon de la
future rendue par `Kernel::run()`** — un garde armé avant que la tâche ne soit
lancée demande l'arrêt à la destruction, et la tâche le mène à terme (§ 0).

`KernelHandle` est clonable et **atteignable** depuis le Container par
l'accesseur `Container::handle()`, que `BootContext` et `RunContext` reflètent —
ce n'est pas une liaison, `container.get::<KernelHandle>()` ne résout rien.
N'importe quelle unité peut donc demander l'arrêt sans qu'un contrat existe.

---

## 14. Observabilité et santé

Le Kernel a besoin d'émettre, mais ne doit dépendre d'aucune implémentation.

```rust
// contract defined by the kernel, implemented by a bundle
pub trait Telemetry: Send + Sync + 'static {
    fn record(&self, record: Record);
}
```

Un `Record` porte un niveau (`Level`), un nom d'événement et des `Field`
ordonnés (`FieldValue`). Le Kernel fournit **trois** implémentations, toutes dans
`kernel-core` et toutes livrées en release : `NoopTelemetry` (le défaut du
builder), `StderrTelemetry` — une ligne par record sur la sortie d'erreur, au
format `NIVEAU événement clé=valeur`, déclaré stable et échappé de sorte qu'un
record tienne toujours sur une ligne — et `RecordingTelemetry`, qui conserve ce
qu'on lui donne (§ 18). Toute implémentation réelle est un Bundle. Chaque
transition de phase, chaque boot de Component, chaque redémarrage de Runnable,
chaque erreur produit un `Record`.

Santé :

```rust
pub trait HealthProbe: Extension {
    fn name(&self) -> &'static str;
    fn check<'a>(&'a self) -> BoxFuture<'a, Health>;
}
```

Le point déclaré par le Kernel n'est pas `HealthProbe` mais le type enveloppe
`Probe` : un point est indexé par le type contribué, donc sans enveloppe commune
chaque implémentation serait son propre point et aucun agrégat ne les atteindrait
tous. Un Bundle contribue `Probe::new(ma_sonde)`.

Le Kernel **agrège** : `aggregate(&ExtensionPoints) -> HealthReport` lance toutes
les sondes en parallèle sur la tâche appelante, chacune bornée par
`PROBE_TIMEOUT` (5 s) — une sonde muette devient un verdict `Down` qui la nomme,
pas un rapport bloqué. `HealthReport { overall, probes }` se rend lui-même en
texte, et `Health` (`Up` / `Degraded` / `Down`, avec `worst` et `worst_of`) est
le treillis qui donne le verdict global. Le Kernel **n'expose pas** : servir cet
état sur un port est le travail d'un Bundle.

---

## 15. Décisions Rust — les points qui décident si ça compile

### Traits utilisables dynamiquement

Toutes les surfaces du Kernel sont dyn-compatibles. Les méthodes asynchrones
retournent un futur boxé via un alias défini par le Kernel :

```rust
pub type BoxFuture<'a, T> = core::pin::Pin<Box<dyn core::future::Future<Output = T> + Send + 'a>>;
```

L'autre moitié de la même décision : `yield_now`, un point de suspension écrit en
une douzaine de lignes de `core`, pour qu'une unité qui boucle rende la main sans
qu'aucune crate de son graphe ne nomme un exécuteur.

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
- `unsafe_code = "forbid"` dans `[workspace.lints.rust]`, hérité par chaque
  membre : c'est la contrainte la plus forte de la liste, et elle est gardée
  (§ 16), pas espérée.
- Édition 2024, `resolver = "3"`, MSRV `1.88.0` déclarée une seule fois dans
  `[workspace.package]` : aucune garde ne la code en dur, `ci/msrv.sh` la lit du
  manifeste et la CI construit et teste sur cette version exacte.
- Deux features Cargo sur `kernel` : `signals` (par défaut) allume la capture des
  signaux, et une tâche de CI construit **et teste** tout sans elle — une
  capacité désactivable jamais testée éteinte ne l'est pas ; `testing` expose le
  point d'accroche de substitution (§ 18).

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

`kernel-macros` ne dépend de **rien** — ni `syn`, ni `quote`, ni `proc-macro2` :
son analyseur de tokens est écrit à la main, parce qu'une crate de macros qui
traînerait un analyseur le mettrait dans chaque build de chaque bundle utilisant
un seul dérive.

---

## 16. Frontière Kernel / Bundle et gardes

### Liste fermée de ce qui est DANS le Kernel

Phases et machine à états · Registry · Container · Contracts (identité et
résolution) · Component · Service (fourniture) · Runnable et supervision ·
ExtensionPoint · EventDispatcher/Listener · arbre de configuration et ses deux
sources (`EnvSource`, `MemorySource`, chaînées) · modèle d'erreur · coordination
d'arrêt, signaux compris · contrat `Telemetry` et ses trois implémentations de
diagnostic · contrat `HealthProbe`, son enveloppe `Probe` et l'**agrégation**
(`aggregate`, `HealthReport`, `PROBE_TIMEOUT`) — mais aucune exposition (§ 14) ·
harnais de test.

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
| Le Kernel ne dépend de rien de métier | `ci/check-dependencies.sh` : liste blanche, une entrée par membre du workspace, un membre absent de la table échoue |
| Aucun `unsafe`, nulle part | `ci/check-lints.sh` : la strophe `[workspace.lints]` est intacte, chaque membre l'hérite, aucun `allow` ne la contourne, et le mot-clé `unsafe` est cherché en clair |
| Aucun nom de domaine dans le Kernel | `grep` en CI sur un glossaire de domaine du projet |
| Bundle ↔ Bundle interdit | `ci/check-bundle-graph.sh` : aucune crate `*-bundle` dans le graphe résolu d'une crate `*-bundle` |
| `requires` conforme à `build` | Instrumentation du Container en build de debug, échec si résolution non déclarée |
| Pas de résolution paresseuse en phase 5 | Toute la table `Shared` est instanciée en phase 4, puis le conteneur est scellé : une première instanciation `Shared` ensuite est `ContainerError::Sealed` |
| Macros non obligatoires | Une tâche de CI compile la suite sans `kernel-macros` |
| Surface publique documentée | `missing_docs = "deny"` dans `[workspace.lints.rust]`, hérité par les quatre crates du Kernel **et** par les exemples ; tenu par `ci/check-lints.sh` et par la tâche `docs` (`rustdoc` avec `-D warnings`) |
| Ce que la licence déclarée promet est présent | `ci/check-licenses.sh` : `MIT OR Apache-2.0` a bien ses deux fichiers, avec leur ligne de copyright |
| Aucune dépendance interdite, vulnérable ou dupliquée | Tâche `deny` : `cargo-deny` (advisories, licences, bans, sources) contre `deny.toml`, `--deny warnings` |
| La MSRV déclarée est la vraie | Tâche `msrv` : build et tests sur la version que `ci/msrv.sh` lit du manifeste |
| `signals` est réellement optionnelle | Tâche `no-default-features` : build **et** tests du workspace sans elle |
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
```

Sa version exécutable est `examples/medium` : mêmes couches, noms réels, et elle tourne.

Point d'entrée :

```rust
fn main() -> ExitCode {
    // Pas `#[tokio::main]` : cette macro termine le programme en détruisant le
    // runtime, et le `Drop` d'un `Runtime` multi-thread joint chacun de ses
    // threads sans borne. Une tâche abandonnée par le Kernel — abandonnée parce
    // qu'elle n'atteint aucun point d'attente, donc n'observe jamais son
    // annulation — retient alors le processus après que l'échelle a terminé et
    // l'a dit. `shutdown_timeout` est cette borne ; construire le runtime à la
    // main est ce qui la rend atteignable.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let code = runtime.block_on(run());
    runtime.shutdown_timeout(Duration::from_secs(5));
    code
}

async fn run() -> ExitCode {
    // `?` n'est pas utilisable ici : `ExitCode` n'implémente pas `FromResidual`.
    // L'échec de construction se rend donc explicitement.
    let kernel = match Kernel::builder()
        .telemetry(Arc::new(StderrTelemetry))
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

- `TestBuilder` : mêmes sept phases, pas de capture de signal, budgets d'arrêt
  courts (50 ms / 100 ms), et un enregistreur de telemetry lisible avant la
  construction. C'est lui qui porte `start`, et `start` est ce qui **produit** le
  harnais ; `TestHarness` conduit ensuite le Kernel avec `stop` et `wait`, et
  l'observe avec `is_running`, `container`, `handle`, `telemetry` et
  `wait_for_record` — cette dernière étant la façon de se synchroniser sur un
  record du Kernel plutôt que de dormir un temps deviné.
- `TestBuilder::keep_running` tient le Kernel ouvert quand rien dans le graphe ne
  tourne. Sans elle, un Bundle qui possède un Component et aucun Runnable n'est
  pas pilotable : la phase 5 publie `Running` et demande l'arrêt dans le même
  souffle, et `start` rend un harnais dont le Container est déjà éteint. C'est la
  forme de Bundle la plus courante.
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
- `kernel-testkit` fournit aussi les **doublures** : `Recorder<T>` (ce qu'on lui
  a passé), `LifecycleLog` et `Call` (les appels de lifecycle reçus), `Parking`
  (un Runnable qui ne fait que rendre la main sur le jeton d'arrêt). Une doublure
  de Component ne s'écrit alors qu'à l'endroit où elle nomme un contrat.
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
  `register` échoue là et le dit, au lieu de se faire passer pour autonome —
  et `missing_contracts_with` pose la même question en fournissant les sources
  qu'il lui faut pour atteindre la phase 3.
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
| 1 | **Rechargement de configuration à chaud** | L'arbre est figé après la phase 1 ; il est immuable et atteignable par l'accesseur `Container::config()`, ce n'est pas une liaison — rien ne le fournit, donc rien ne le résout |
| 2 | **Chargement dynamique de Bundles** | Les Bundles sont liés à la compilation ; `KernelBuilder::bundle` prend une valeur, pas un chemin |
| 3 | **Kernels multiples ou imbriqués dans un processus** | Rien n'interdit techniquement deux `Kernel` — aucun état global mutable —, mais aucune coordination n'est fournie ni testée, et le second doit couper `capture_signals`, qui vaut `true` par défaut |
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
| 1 | Runtime | Le Kernel ne le crée pas ; il est une `Future`, conduite sur Tokio |
| 2 | Phases | Sept, ordre strict, aucune contournée ; un échec termine la séquence |
| 3 | Register / Resolve | Deux phases séparées ; `register` est sync et aveugle |
| 4 | Container | Résolution dynamique, effacement de type, validée en phase 3 |
| 5 | Résolution paresseuse | Aucune première instanciation `Shared` après le sceau ; `Scoped` et `Factory` restent construits à la demande sur un graphe validé en phase 3 ; garde `debug_assertions` sur ce qu'un provider ou une unité déclare |
| 6 | Contracts | Traits en crate séparée ; seul canal de **résolution** entre Bundles, les deux autres étant `ExtensionPoint` et événement |
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
| 18 | Observabilité | Contrat `Telemetry` défini par le Kernel, avec trois implémentations dans `kernel-core` (`Noop`, `Stderr`, `Recording`) ; toute sortie réelle est un Bundle |
| 19 | Rust async | `BoxFuture` partout aux frontières ; `Send + Sync + 'static` universel |
| 20 | Macros | Aucune n'est requise ; vérifié par une tâche de CI |
