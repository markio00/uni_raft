# Fatti a cui prestare attenzione
1. Quando il paper, nella sezione 6, sostiene che il leader non debba includersi nel conteggio delle maggioranze,
   intende probabilmente solo la maggioranza necessaria per il commit di C_{new}.

1. Non sembra sia necessario che un leader per il term _t_ si preoccupi del commit di entry il cui term precede _t_.

1. Il leader deve tenere traccia dell'ultima committed entry e la deve includere nelle chiamate ad _AppendEntry_.
   Quando un follower capisce che una entry è committed la deve applicare alla propria state machine seguendo l'ordine del log.

1. L'RPC _AppendEntries_ deve includere un qualsiasi identificatore del leader, affinchè questo possa essere contattato.

1. Serve davvero un numero seriale per le chiamate del client?

1. Il tempo di broadcast (cioè il tempo medio che un server impiega per inviare le RPC in parallelo e ricevere le risposte) deve essere almeno
   un ordine di grandezza inferiore al timeout per le elezioni, che a sua volta deve essere un paio di ordini di grandezza inferiore al MTBF di un server.

# Proprietà garantite da Raft
1. **election safety**: in un certo term può essere eletto al massimo un leader
1. **leader append only**: un leader non cancella nè sovrascrive le entry del proprio log, ma esegue unicamente aggiunte alla fine
1. **log matching**: se due log contengono un'entry avente uguale indice _i_ e uguale term, allora i due log sono identici fino all'indice _i_
1. **leader completeness**: se un'entry viene committed in un term, allora quell'entry sarà nei log dei leader per tutti i successivi term
1. **state machine safety**: se un server ha applicato una log entry di indice _i_ alla state machine, allora nessun altro nodo applicherà mai una diversa entry con indice _i_.

# Critical points
1. If _votedFor_ is not persisted on disk, then a node m which has already voted for a node n_{1} may crash, forget its vote, turn up again and vote a second time for another node n_{2}, violating the election safety property (if two nodes get the majority at the same time).
1. Quand'è che un nodo assume diritto di voto? Quando ha "catchato up" con il vecchio log, okay...ma come riesco a capirlo programmaticamente?
1. Dopo che ho votato resetto il timer perchè così evito la spiacevole situazione in cui a un altro nodo scade il timer, il nodo chiama le elezioni, poi il timer scade anche a me e devo ricominciare le elezioni anche se ho appena votato
1. Come fare coesistere il thread "ciclo di vita" con il thread "gestione RPC"? In altre parole, dove vanno inseriti i lock?
1. Come codificare la condizione che un nodo, dopo avere inviato una serie di richieste in parallelo, debba sì attendere le risposte ma solo entro un certo limite di tempo?
1. Se un server S riceve N chiamate di procedura remota, queste N vengono eseguite in maniera concorrente o FIFO? Nel primo caso bisogna prestare attenzione alle corse critiche sullo stato di S, dovute alle N chiamate parallele. RISOLTO: in maniera concorrente.
1. Bisogna fare in modo di gestire le richieste del client in modo che arrivino un minimo distanziate (evitando DOS)

# Approcci alla gestione delle richieste durante il ciclo di vita del leader

## Thread effimeri
Un thread per ogni richiesta del leader, con timeout.

## Worker thread perpetui (uno per ogni follower)
Maggiormente error-prone (in quanto a errori logici).
Comodo per il cambio di configurazione, in quanto l'aggiunta di un nodo implica solamente (?) l'aggiunta di un worker. 

## Un thread perpetuo che ha come solo scopo inviare heartbeat in parallelo, che coesiste con il loop del leader
Scelto per "separation of concerns".

> Lo scope delle goroutine è vincolato alla durata di vita del main.
>
> Ecco perchè serve sincronizzarsi con canali e/o wait group.

# Da dove si riparte?
Dal conteggio di maggioranze distinte
