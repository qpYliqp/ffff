package crtracker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

class Deck implements Serializable {
    public Set<String> players;  // Changement ArrayList -> HashSet
    private Map<String, Integer> evos;
    private Map<String, Integer> wevos;
    private Map<String, Integer> towers;
    private Map<String, Integer> wtowers;
	public String id;
	int count = 0;
	int win = 0;
	double strength = 0;
	int nplayers = 0;
	int league = 0;
	int trophy = 0;

  public Deck(String str, String evo, String tower, int count, int win, 
                double strength, String player, int league, int trophy) {
        // Pré-allouer les collections avec la taille attendue
        this.players = new HashSet<>(11);  // Capacité initiale pour max 10 éléments
        this.evos = new HashMap<>(evo.length() / 2);
        this.wevos = new HashMap<>(evo.length() / 2);
        this.towers = new HashMap<>(tower.length() / 2);
        this.wtowers = new HashMap<>(tower.length() / 2);
        
        char[] chars = str.toCharArray();
        String[] tmp = new String[chars.length / 2];
        for (int i = 0; i < chars.length; i += 2) {
            tmp[i/2] = new String(chars, i, 2);
        }
        Arrays.sort(tmp);
        this.id = String.join("", tmp);
        
        this.count = count;
        this.win = win;
        this.strength = strength;
        this.league = league;
        this.trophy = trophy;
        this.players.add(player);
        

        processCards(evo, evos, wevos, win > 0);
        processCards(tower, towers, wtowers, win > 0);
    }
    
    private void processCards(String cards, Map<String, Integer> cardMap, 
                            Map<String, Integer> winMap, boolean isWin) {
        for (int i = 0; i < cards.length(); i += 2) {
            String key = cards.substring(i, i + 2);
            cardMap.put(key, 1);
            winMap.put(key, isWin ? 1 : 0);
        }
    }


    public Deck merge(Deck b) {
        this.count += b.count;
        this.win += b.win;
        this.strength += b.strength;
        this.league = Math.max(b.league, this.league);
        this.trophy = Math.max(b.trophy, this.trophy);
        
        if (this.players.size() < 10) {
            for (String player : b.players) {
                if (this.players.size() >= 10) break;
                this.players.add(player);
            }
        }
        
    
        mergeMaps(this.evos, this.wevos, b.evos, b.wevos);
        mergeMaps(this.towers, this.wtowers, b.towers, b.wtowers);
        
        return this;
    }
    
    private void mergeMaps(Map<String, Integer> map1, Map<String, Integer> wmap1,
                          Map<String, Integer> map2, Map<String, Integer> wmap2) {
        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
            String key = entry.getKey();
            map1.merge(key, entry.getValue(), Integer::sum);
            wmap1.merge(key, wmap2.get(key), Integer::sum);
        }
    }

	public String toString() {
		double winrate = 0;
		double stren = 0;
		if (count > 0) {
			winrate = Math.round(win * 1000 / count) / 10.;
			stren = strength / count;
		}

		String sevo = "[";
		boolean first = true;
		for (String key : evos.keySet()) {
			if (!first)
				sevo += ",";
			first = false;
			sevo += "['" + key + "'," + evos.get(key) + "," + Math.round(wevos.get(key) * 1000. / evos.get(key)) / 10.
					+ "]";
		}
		sevo += ']';

		String stower = "[";
		first = true;
		for (String key : towers.keySet()) {
			if (!first)
				stower += ",";
			first = false;
			stower += "['" + key + "'," + towers.get(key) + ","
					+ Math.round(wtowers.get(key) * 1000. / towers.get(key)) / 10. + "]";
		}
		stower += ']';

		return "{'id':'" + id + "', 'evos':" + sevo + ", 'towers':" + stower
				+ ", 'count':" + count + ", 'winrate':" + winrate + ", 'nbplayers':" + players.size() + ", 'win':" + win
				+ ", 'strength':" + stren
				+ ", 'league':" + league
				+ ", 'ctrophy':" + trophy + "}";
	}

}